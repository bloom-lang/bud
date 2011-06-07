require 'test_common'

# TODO:
# * init token as an event
# * test failure / node death

# Somewhat ugly: we want to initiate events that are outside the scope of
# auto-ft (e.g., we don't want "please shutdown" messages to be replayed after
# node failure).
module ChildMetaProtocol
  state do
    channel :chn_shutdown, [:@addr]
    channel :chn_init, [:@addr]
  end
end

# Instead of a token ring, form a "line" of nodes
module TokenLineChild
  include ChildMetaProtocol

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
  end
end

class TokenLineAft
  include Bud
  include AftDeploy
  include ChildMetaProtocol

  state do
    scratch :do_shutdown, [:node_id]
    scratch :do_init, [:node_id]
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
  end
end

class TestAutoFt < Test::Unit::TestCase
  def test_line_with_kill
    b = TokenLineAft.new
    b.run_bg
    b.stop_bg
  end
end
