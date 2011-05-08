require 'bud/deploy/forkdeploy'

FT_TIMEOUT = 20
AFT_MSG_ID = -1

module AftProtocol
  state do
    # Liveness ping messages from child => master
    channel :ping_chan, [:@loc, :node_id]

    # Messaging abstraction: child => master (send), master => child (recv)
    channel :msg_send, [:@loc, :msg_id, :recv_node, :send_node] => [:payload]
    channel :msg_recv, [:@loc, :msg_id, :recv_node, :send_node] => [:payload]
  end
end

module AftChild
  include AftProtocol

  state do
    periodic :ping_clock, 5
    scratch :aft_send, [:recv_node] => [:payload]
    scratch :aft_recv, [:send_node, :msg_id] => [:payload]
  end

  bloom :send_ping do
    ping_chan <~ ping_clock {|c| [@deployer_addr, @node_id]}
  end

  bloom :messaging do
    msg_send <~ aft_send {|m| [@deployer_addr, AFT_MSG_ID, m.recv_node, @node_id, m.payload]}

    aft_recv <= msg_recv do |m|
      raise if m.recv_node != @node_id
      [m.send_node, m.msg_id, m.payload]
    end
  end
end

# XXX: Currently, this code runs at both the deployment master and at all the
# child nodes. Running at the children is obviously inefficient, but requires
# some refactoring of the deployment infrastructure. See #147.
module AftMaster
  include AftProtocol

  state do
    table :last_ping, [:node_id] => [:tstamp]
    scratch :new_ping, last_ping.schema
    scratch :not_live, [:node_id]
    periodic :ft_clock, 2
  end

  bloom :check_liveness do
    # NB: This rule doesn't include nodes that have never sent a ping
    not_live <= (ft_clock * last_ping).pairs do |c, p|
      [p.node_id] if (c.val - FT_TIMEOUT > p.tstamp)
    end
    stdio <~ ft_clock {|c| ["Got FT clock tick (pid = #{Process.pid})"]}
    stdio <~ not_live {|n| ["Dead node: id = #{n.node_id}"]}
  end

  bloom :handle_ping do
    # We assign ping timestamps at the deployer, to avoid sensitivity to
    # node-local clock skew.
    new_ping <= ping_chan {|p| [p.node_id, Time.now]}
    last_ping <+ new_ping
    last_ping <- (new_ping * last_ping).rights(:node_id => :node_id)
  end

  bloom :message_redirect do
    msg_send <~ (msg_recv * node).pairs(:recv_node => :uid) do |m,n|
      [n.addr, m.msg_id, m.recv_node, m.send_node, m.payload]
    end
  end
end

module AftDeploy
  include ForkDeploy
  include AftMaster

  def initialize(opts={})
    super
    @child_modules << "AftChild"
  end
end
