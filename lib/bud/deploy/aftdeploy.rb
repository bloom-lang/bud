require 'bud/deploy/forkdeploy'

FT_TIMEOUT = 20

module AftProtocol
  state do
    # Liveness ping messages from child => master
    channel :ping_chan, [:@loc, :attempt_id]

    # Successful child startup (child => master)
    channel :child_ack, [:@loc, :attempt_id] => [:addr]

    # Messaging abstraction: child => master (send), master => child (recv).
    # "send_id" records the sequence of messages emitted by a given node;
    # "recv_id" records the sequence of messages received by a given node.
    channel :msg_send, [:@loc, :send_id, :recv_node, :send_node] => [:payload]
    channel :msg_recv, [:@loc, :recv_id, :recv_node, :send_node] => [:payload]
  end
end

module AftChild
  include AftProtocol

  state do
    periodic :ping_clock, 3
    table :next_send_id, [] => [:send_id]
    # All messages with IDs <= recv_done_max have been delivered to user code
    # (emitted via aft_recv).
    table :recv_done_max, [] => [:recv_id]
    table :recv_buf, msg_recv.schema
    scratch :deliver_msg, recv_buf.schema
    loopback :do_tick, [] => [:do_it]

    table :got_atomic_data, [] => [:t]

    scratch :aft_send, [:recv_node] => [:payload]
    # Note that we provide ordered, reliable delivery: messages will be emitted
    # via aft_recv in strictly-increasing msg_id order (no gaps).
    scratch :aft_recv, [:send_node, :msg_id] => [:payload]
  end

  bootstrap do
    child_ack <~ [[@deployer_addr, @attempt_id, ip_port]]
    next_send_id <= [[0]]
    recv_done_max <= [[-1]]
  end

  bloom :send_ping do
    ping_chan <~ ping_clock {|c| [@deployer_addr, @attempt_id]}
  end

  bloom :send_msg do
    # XXX: we assume no message batching
    msg_send <~ (aft_send * next_send_id).pairs do |m, n|
      [@deployer_addr, n.send_id, m.recv_node, @node_id, m.payload]
    end

    next_send_id <+ (aft_send * next_send_id).rights {|n| [n.send_id + 1]}
    next_send_id <- (aft_send * next_send_id).rights
  end

  bloom :recv_msg do
    recv_buf <= msg_recv do |m|
      if m.recv_node != @node_id
        raise "Node mismatch: got #{m.recv_node}, expected #{@node_id} (@ #{ip_port})"
      end
      m
    end

    # XXX: hacky. We need to ensure we don't start delivering data until the EDB
    # (initial_data) has been received
    got_atomic_data <+ atomic_data_out {|a| [true]}
    do_tick <~ atomic_data_out {|a| [true]}

    deliver_msg <= (recv_buf * recv_done_max).pairs do |b, m|
      b if b.recv_id == (m.recv_id + 1) and not got_atomic_data.empty?
    end
    recv_done_max <+ (deliver_msg * recv_done_max).rights {|m| [m.recv_id + 1]}
    recv_done_max <- (deliver_msg * recv_done_max).rights
    recv_buf <- deliver_msg
    do_tick <~ deliver_msg {|m| [true]}

    aft_recv <= deliver_msg do |m|
      [m.send_node, m.recv_id, m.payload]
    end
  end
end

ATTEMPT_INIT = 1        # Attempt created, but no process spawned yet
ATTEMPT_FORK = 2        # Process spawned, but no messages received from it yet
ATTEMPT_LIVE = 3        # Process is currently running
ATTEMPT_DEAD = 4        # Process is (presumed to be) dead

# XXX: Currently, this code runs at both the deployment master and at all the
# child nodes. Running at the children is obviously inefficient, but requires
# some refactoring of the deployment infrastructure. See #147.
module AftMaster
  include AftProtocol
  include Deployer

  def initialize(opts={})
    super
    @child_modules = [AftChild]
    @child_pids = []
    @dead_pids = []
  end

  state do
    # Record the latest attempt to run each node
    table :node_status, [:node_id] => [:attempt_id]

    # The status of all attempts ever made
    table :attempt_status, [:attempt_id] => [:node_id, :status, :addr, :last_ping]

    # Wrap side-effecting calls to fork()
    scratch :fork_req, [:attempt_id] => [:node_id]
    scratch :fork_done, [:attempt_id]

    # Buffer all messages, in case we later need to replay them
    table :msg_buf, [:send_node, :send_id] => [:recv_node, :recv_id, :payload]
    table :next_recv_id, [:node_id] => [:recv_id]

    scratch :do_msg, msg_send.schema
    scratch :new_msg, msg_buf.schema
    scratch :new_ping, [:attempt_id, :tstamp]
    scratch :not_live, [:attempt_id]
    periodic :ft_clock, 2
  end

  bootstrap do
    return unless @options[:deploy]

    Signal.trap("CHLD") do
      # We receive SIGCHLD when a child process changes state; unfortunately,
      # there's no easy way to tell whether the child process we're getting the
      # signal for is one of ForkDeploy's children. Hence, check if any of the
      # forked children have exited. We also ignore Errno::ECHILD, because
      # someone else's waitpid() could easily race with us.
      @child_pids.each do |c|
        begin
          pid = Process.waitpid(c, Process::WNOHANG)
          unless pid.nil?
            @dead_pids << pid
          end
        rescue Errno::ECHILD
        end
      end
    end

    on_shutdown do
      # NB: Setting the SIGCHLD handler to "IGNORE" results in waitpid() being
      # called automatically (to cleanup zombies), at least on OSX. This is not
      # what we want, since it would cause a subsequent waitpid() to fail.
      Signal.trap("CHLD", "DEFAULT")
      pids = @child_pids - @dead_pids
      pids.each do |p|
        begin
          Process.kill("TERM", p)
          Process.waitpid(p)
        rescue Errno::ESRCH
        end
      end
    end

    register_callback(:fork_req) do |tbl|
      tbl.each do |t|
        @child_pids << do_fork(t.attempt_id, t.node_id)
        fork_done <+ [[t.attempt_id]]
      end
    end

    # Create initial attempts for all of the configured nodes. During bootstrap,
    # we set their status to INIT; this is later replaced with FORK once we
    # actually spawn the attempt processes.
    node_count[[]].num.times do |i|
      # Use the node ID as the initial attempt ID
      node_status << [i, i]
      attempt_status << [i, i, ATTEMPT_INIT, nil, bud_clock]
      next_recv_id << [i, 0]
    end
  end

  def do_fork(attempt_id, node_id)
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deployer_addr = ip_port
    Bud.do_fork do
      @child_modules.each do |m|
        # XXX: Can this be done without "instance_eval"?
        self.class.instance_eval "include #{m}"
      end
      child = self.class.new(child_opts)
      child.instance_variable_set('@deployer_addr', deployer_addr)
      child.instance_variable_set('@attempt_id', attempt_id)
      child.instance_variable_set('@node_id', node_id)
      child.run_fg
    end
  end

  bloom :do_fork do
    fork_req <= attempt_status do |s|
      [s.attempt_id, s.node_id] if s.status == ATTEMPT_INIT
    end
    attempt_status <+ attempt_status do |s|
      [s.attempt_id, s.node_id, ATTEMPT_FORK, s.addr, s.last_ping] if s.status == ATTEMPT_INIT
    end
    attempt_status <- attempt_status do |s|
      s if s.status == ATTEMPT_INIT
    end
  end

  bloom :move_to_live do
    # Update attempt status to LIVE and add child to "node" when child_ack
    # received, unless we've already declared the attempt to be DEAD
    # XXX: cleanup this code
    attempt_status <+ (attempt_status * child_ack).pairs(:attempt_id => :attempt_id) do |as, ack|
      [as.attempt_id, as.node_id, ATTEMPT_LIVE, ack.addr, bud_clock] if as.status == ATTEMPT_FORK
    end
    attempt_status <- (attempt_status * child_ack).lefts(:attempt_id => :attempt_id) do |as|
      as if as.status == ATTEMPT_FORK
    end
    node <+ (attempt_status * child_ack).pairs(:attempt_id => :attempt_id) do |as, ack|
      [as.node_id, ack.addr] if as.status == ATTEMPT_FORK
    end
    node_ready <+ (attempt_status * child_ack).pairs(:attempt_id => :attempt_id) do |as, ack|
      [as.node_id] if as.status == ATTEMPT_FORK
    end

    # Replay all buffered messages for the new attempt for this node
    msg_recv <~ (attempt_status * child_ack * msg_buf).combos(attempt_status.attempt_id => child_ack.attempt_id, msg_buf.recv_node => attempt_status.node_id) do |as, ack, m|
      puts "Resending message: #{[ack.addr, m.recv_id, m.recv_node, m.send_node, m.payload].inspect}" if as.status == ATTEMPT_FORK
      [ack.addr, m.recv_id, m.recv_node, m.send_node, m.payload] if as.status == ATTEMPT_FORK
    end
  end

  bloom :check_liveness do
    not_live <= (ft_clock * attempt_status).pairs do |c, as|
      [as.attempt_id] if [ATTEMPT_FORK, ATTEMPT_LIVE].include? as.status and (c.val - FT_TIMEOUT > as.last_ping)
    end
    stdio <~ not_live {|n| ["Dead node: attempt id = #{n.attempt_id}"]}

    # Mark the old attempts as dead
    attempt_status <+ (not_live * attempt_status).matches.rights do |as|
      [as.attempt_id, as.node_id, ATTEMPT_DEAD, as.addr, as.last_ping]
    end
    attempt_status <- (not_live * attempt_status).matches.rights

    # Remove old attempts from "node"
    node <- (node * not_live * attempt_status).combos(not_live.attempt_id => attempt_status.attempt_id, node.uid => attempt_status.node_id) do |n, nl, as|
      n
    end

    # Create new attempts for the failed nodes
    # XXX: attempt_id assignment is a hack
    attempt_status <+ (not_live * attempt_status).matches.rights do |as|
      [as.attempt_id + 10, as.node_id, ATTEMPT_INIT, nil, bud_clock]
    end

    # Update "node_status" to point at the newly-created attempts
    # XXX: attempt_id assignment is a hack
    node_status <+ (not_live * attempt_status).matches.rights do |as|
      [as.node_id, as.attempt_id + 10]
    end
    node_status <- (not_live * node_status).matches.rights
  end

  bloom :handle_ping do
    # We assign ping timestamps at the deployer, to avoid sensitivity to clock
    # skew between child nodes. Note that we accept and apply timestamp updates
    # for all attempts, even if they have been declared dead.
    new_ping <= ping_chan {|p| [p.attempt_id, bud_clock]}
    attempt_status <+- (attempt_status * new_ping).matches do |as, p|
      [as.attempt_id, as.node_id, as.status, as.addr, p.tstamp]
    end
  end

  bloom :handle_messages do
    # When we receive a message from a node, first check if we've already seen a
    # previous message with the same (send_node, send_id). If not, assign the
    # message a new recv_id. This ID fixes the position of the message in the
    # delivery order for the message's recipient node.
    # XXX: we assume that message batching does not occur
    do_msg <= msg_send do |m|
      b = msg_buf[[m.send_node, m.send_id]]
      unless b.nil?
        puts "Skipped duplicate message: #{m.inspect}"
        # Sanity check
        if b.recv_node != m.recv_node or b.payload != m.payload
          raise "Replay error: previous msg #{b.inspect}, new msg = #{m.inspect}"
        end
      end
      m if b.nil?
    end
    new_msg <= (do_msg * next_recv_id).pairs(:recv_node => :node_id) do |m, n|
      [m.send_node, m.send_id, m.recv_node, n.recv_id, m.payload]
    end
    next_recv_id <+- (do_msg * next_recv_id).rights(:recv_node => :node_id) do |n|
      [n.node_id, n.recv_id + 1]
    end
    msg_buf <+ new_msg

    msg_recv <~ (new_msg * node_status * attempt_status).combos(new_msg.recv_node => node_status.node_id, node_status.attempt_id => attempt_status.attempt_id) do |m, ns, as|
      [as.addr, m.recv_id, m.recv_node, m.send_node, m.payload] if as.status == ATTEMPT_LIVE
    end
  end
end

module AftDeploy
  include AftMaster
end
