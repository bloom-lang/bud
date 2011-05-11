require 'bud/deploy/forkdeploy'

FT_TIMEOUT = 15

module AftProtocol
  state do
    # Liveness ping messages from child => master
    channel :ping_chan, [:@loc, :attempt_id]

    # Successful child startup (child => master)
    channel :child_ack, [:@loc, :attempt_id] => [:addr]

    # Messaging abstraction: child => master (send), master => child (recv)
    channel :msg_send, [:@loc, :msg_id, :recv_node, :send_node] => [:payload]
    channel :msg_recv, [:@loc, :msg_id, :recv_node, :send_node] => [:payload]
  end
end

module AftChild
  include AftProtocol

  def initialize(opts={})
    super
    @message_id = 0
  end

  state do
    periodic :ping_clock, 3
    scratch :aft_send, [:recv_node] => [:payload]
    scratch :aft_recv, [:send_node, :msg_id] => [:payload]
  end

  # Send a ping on bootup to let the master node know we're alive
  bootstrap do
    ping_chan <~ [[@deployer_addr, @attempt_id]]
  end

  bloom :send_ping do
    ping_chan <~ ping_clock {|c| [@deployer_addr, @attempt_id]}
  end

  bloom :messaging do
    msg_send <~ aft_send {|m| [@deployer_addr, next_msg_id, m.recv_node, @node_id, m.payload]}

    aft_recv <= msg_recv do |m|
      raise if m.recv_node != @node_id
      [m.send_node, m.msg_id, m.payload]
    end
  end

  # XXX: It would be cleaner to assign message IDs using Bloom code.
  def next_msg_id
    @message_id += 1
    @message_id
  end
end

ATTEMPT_INIT = 1
ATTEMPT_FORK = 2
ATTEMPT_LIVE = 3
ATTEMPT_DEAD = 4

# XXX: Currently, this code runs at both the deployment master and at all the
# child nodes. Running at the children is obviously inefficient, but requires
# some refactoring of the deployment infrastructure. See #147.
module AftMaster
  include AftProtocol
  include Deployer

  def initialize(opts={})
    super
    @child_modules = [AftChild]
    @attempt_id_counter = 0
    @child_pids = []
    @dead_pids = []
  end

  state do
    # Keep track of the latest attempt to run each node
    table :node_status, [:node_id] => [:attempt_id]

    # The status of all attempts, ever. Note that once an attempt is declared
    # "dead", we ignore all subsequent pings and messages from it.
    table :attempt_status, [:attempt_id] => [:node_id, :status, :addr, :last_ping]

    # Wrap side-effecting calls to fork()
    scratch :fork_req, [:attempt_id] => [:node_id]
    scratch :fork_done, [:attempt_id]

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
        pid = do_fork(t.attempt_id, t.node_id)
        @child_pids << pid
        fork_done <+ [[t.attempt_id]]
      end
    end

    # Create initial attempts for all of the configured nodes. During bootstrap,
    # we just set their status to INIT; this is later replaced with FORK once we
    # actually spawn the attempt processes.
    node_count[[]].num.times do |i|
      # Use the node ID as the initial attempt ID
      node_status << [i, i]
      attempt_status << [i, i, ATTEMPT_INIT, nil, bud_clock]
    end
  end

  def do_fork(attempt_id, node_id)
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deployer_addr = ip_port
    @child_pids << Bud.do_fork do
      @child_modules.each do |m|
        # XXX: Can this be done without "instance_eval"?
        self.class.instance_eval "include #{m}"
      end
      child = self.class.new(child_opts)
      child.instance_variable_set('@deployer_addr', deployer_addr)
      child.instance_variable_set('@attempt_id', attempt_id)
      child.instance_variable_set('@node_id', node_id)
    end
  end

  bloom :spawn_children do
    fork_req <= attempt_status do |s|
      [s.attempt_id, s.node_id] if s.status == ATTEMPT_INIT
    end
    attempt_status <+ attempt_status do |s|
      [s.attempt_id, s.node_id, ATTEMPT_FORK, s.addr, s.last_ping] if s.status == ATTEMPT_INIT
    end
    attempt_status <- attempt_status do |s|
      s if s.status == ATTEMPT_INIT
    end

    # Update attempt status when child ACK message received
    attempt_status <+ (attempt_status * child_ack).pairs(:attempt_id => :attempt_id) do |as, ack|
      raise if as.status != ATTEMPT_FORK
      [as.attempt_id, as.node_id, ATTEMPT_LIVE, ack.addr, bud_clock]
    end
    attempt_status <- (attempt_status * child_ack).lefts(:attempt_id => :attempt_id)
  end

  bloom :check_liveness do
    not_live <= (ft_clock * attempt_status).pairs do |c, as|
      [as.attempt_id] if [ATTEMPT_FORK, ATTEMPT_LIVE].include? as.status and (c.val - FT_TIMEOUT > as.last_ping)
    end
    stdio <~ not_live {|n| ["Dead node: attempt id = #{n.attempt_id}"]}

    # Mark the old attempt as dead
    attempt_status <+ (not_live * attempt_status).matches.rights do |as|
      [as.attempt_id, as.node_id, ATTEMPT_DEAD, as.addr, as.last_ping]
    end
    attempt_status <- (not_live * attempt_status).matches.rights
  end

  bloom :handle_ping do
    # We assign ping timestamps at the deployer, to avoid sensitivity to clock
    # skew between child nodes. Note that we accept and apply timestamp updates
    # for all attempts, even if they have been declared dead.
    new_ping <= ping_chan {|p| [p.attempt_id, bud_clock]}
    attempt_status <+ (attempt_status * new_ping).matches do |as, p|
      [as.attempt_id, as.node_id, as.status, as.addr, p.tstamp]
    end
    attempt_status <- (attempt_status * new_ping).matches.lefts
  end

  bloom :message_redirect do
    msg_recv <~ (msg_send * node).pairs(:recv_node => :uid) do |m,n|
      [n.addr, m.msg_id, m.recv_node, m.send_node, m.payload]
    end
  end
end

module AftDeploy
  include AftMaster
end
