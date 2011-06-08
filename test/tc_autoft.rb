require 'test_common'

# TODO:
# * init token as an event
# * test failure / node death

# This protocol is used to initiate events that are outside the scope of auto-ft
# (e.g., we don't want "please shutdown" messages to be replayed after node
# failure).
module ChildMetaProtocol
  state do
    channel :chn_shutdown, [:@addr]
    channel :chn_init, [:@addr]

    # Tell the node to send ACKs to "ack_addr" whenever it receives the token
    channel :chn_ack_req, [:@addr, :ack_addr]
    channel :chn_ack, [:@addr, :payload]
  end
end

# Instead of a token ring, form a "line" of nodes
module TokenLineChild
  include ChildMetaProtocol

  state do
    table :send_ack, [] => [:addr]
  end

  bloom :pass_token do
    aft_send <= (aft_recv * node_count).pairs do |r, nc|
      succ_node = @node_id + 1
      [succ_node, r.payload + 1] unless succ_node == nc.num
    end
  end

  bloom :print_token do
    stdio <~ aft_recv {["#{@node_id}: Got token! (@ #{ip_port})"]}
  end

  bloom :init_token do
    aft_send <= (chn_init * node_count).rights do |nc|
      succ_node = @node_id + 1
      [succ_node, 0] unless succ_node == nc.num
    end
  end

  bloom :send_ack do
    send_ack <+ chn_ack_req {|c| [c.ack_addr]}
    chn_ack <~ (send_ack * aft_recv).pairs do |a, r|
      [a.addr, r.payload]
    end
  end
end

class TokenLineAft
  include Bud
  include AftDeploy
  include ChildMetaProtocol

  state do
    scratch :do_shutdown, [:node_id]
    scratch :do_init, [:node_id]
    scratch :do_ack_req, [:node_id]
  end

  def initialize(opts={})
    super
    @child_modules << TokenLineChild
  end

  bloom :setup_edb do
    initial_data <= (node * node_count).pairs do |n, nc|
      [n.uid, :node_count, [[nc.num]]]
    end
  end

  bloom :meta_commands do
    chn_shutdown <~ (do_shutdown * node).rights(:node_id => :uid) do |n|
      [n.addr]
    end

    chn_init <~ (do_init * node).rights(:node_id => :uid) do |n|
      [n.addr]
    end

    chn_ack_req <~ (do_ack_req * node).rights(:node_id => :uid) do |n|
      [n.addr, ip_port]
    end
  end

  def send_init
    sync_do {
      do_init <+ [[0]]
    }
  end

  def setup_ack_req {
    sync_do {
      do_ack_req <+ [[100000]]
    }
  }
end

class TestAutoFt < Test::Unit::TestCase
  def test_line_basic
    b = TokenLineAft.new
    b.run_bg
    b.setup_ack_req
    b.send_init
    b.register_callback(:chn_ack) do |tbl|
      puts "GOT CALLBACK!"
    end
    b.stop_bg
  end
end
