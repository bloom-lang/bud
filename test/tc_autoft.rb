require 'test_common'

# TODO:
# * init token as an event
# * test failure / node death

# Instead of a token ring, form a "line" of nodes
module TokenLineChild
  bloom :pass_token do
    aft_send <= (aft_recv * node_count).pairs do |r, nc|
      succ_node = @node_id + 1
      [succ_node, r.payload + 1] unless succ_node == nc.num
    end
  end

  bloom :print_token do
    stdio <~ aft_recv {["#{@node_id}: Got token! (@ #{ip_port})"]}
  end
end

class TokenLineAft
  include Bud
  include AftDeploy

  def initialize(opts={})
    super
    @child_modules << TokenLineChild
  end

  bloom :setup_edb do
    initial_data <= (node * node_count).pairs do |n, nc|
      [n.uid, :node_count, [[nc.num]]]
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
