require 'rubygems'
require 'bud'

module TokenRingAft
  state do
    channel :token, [:@loc]
  end

  bloom :make_ring do
    initial_data <= node do |n|
      # Calculate the successor node
      succ_id = (n.uid + 1) % node_count[[]].num
      succ_addr = node[[succ_id]].addr
      [n.uid, :next_node, [[succ_addr]]]
    end
    initial_data <= (node * node_count).pairs do |n, nc|
      [n.uid, :node_count, [[nc.num]]]
    end
  end

  bloom :init_token do
    # The deployer sends an initial message to the node with ID 0
    token <~ (node_ready * node).rights do |n|
      [n.addr] if (@options[:deploy] and n.uid == 0)
    end
  end
end

module TokenRingAftChild
  state do
    table :next_node, [] => [:addr]
    table :token_persist, [:loc]
    periodic :child_tik, 2
  end

  bloom :pass_token do
    # Persist the token for as long as necessary
    token_persist <= aft_recv { ip_port }
    token_persist <= token
    token_persist <- (token_persist * next_node).lefts
    # Pass on the token
    # XXX: we shouldn't need to include "node_count" in the join
    aft_send <= (token_persist * next_node * node_count).combos do |tp, nn, nc|
      [(@node_id + 1) % nc.num, 0]
    end
    stdio <~ (token_persist * next_node * node_count).combos do |tp, nn, nc|
      ["Sending message (#{ip_port})"]
    end
    stdio <~ child_tik { ["Child tick: nn = #{next_node.to_a.inspect}, node_count = #{node_count.to_a.inspect}, token_persist = #{token_persist.to_a.inspect} (@ #{ip_port})"]}
  end

  bloom :print_token do
    stdio <~ (token_persist * next_node).pairs {["#{ip_port}: Got token!"]}
  end
end

class RingAft
  include Bud
  include TokenRingAft
  include AftDeploy

  def initialize(opts={})
    super
    @child_modules << "TokenRingAftChild"
  end

  deploystrap do
    node_count << [10]
  end
end

RingAft.new(:deploy => true).run_fg
