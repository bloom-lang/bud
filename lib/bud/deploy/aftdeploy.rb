require 'bud/deploy/forkdeploy'

FT_TIMEOUT = 15

module AftProtocol
  state do
    # Liveness ping messages from child => master
    channel :ping_chan, [:@loc, :attempt_id]

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

NODE_LIVE = 1
NODE_DEAD = 2
NODE_RESPAWNING = 3

# XXX: Currently, this code runs at both the deployment master and at all the
# child nodes. Running at the children is obviously inefficient, but requires
# some refactoring of the deployment infrastructure. See #147.
module AftMaster
  include AftProtocol

  state do
    table :attempt_status, [:attempt_id] => [:node_id, :status, :addr, :last_ping]
    scratch :new_ping, [:attempt_id, :tstamp]
    scratch :not_live, [:attempt_id]
    periodic :ft_clock, 2
  end

  bloom :init_status do
    attempt_status <= (node * node_ready).lefts do |n|
      # Use node ID as initial attempt ID
      [n.uid, n.uid, NODE_LIVE, n.addr, bud_clock]
    end
  end

  bloom :check_liveness do
    not_live <= (ft_clock * attempt_status).pairs do |c, as|
      [as.attempt_id] if (c.val - FT_TIMEOUT > as.last_ping)
    end
    stdio <~ not_live {|n| ["Dead node: attempt id = #{n.attempt_id}"]}
  end

  bloom :handle_ping do
    # We assign ping timestamps at the deployer, to avoid sensitivity to
    # node-local clock skew.
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
  include ForkDeploy
  include AftMaster

  def initialize(opts={})
    super
    @child_modules << AftChild
  end
end
