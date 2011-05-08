require 'rubygems'
require 'bud'

module TokenRingAft
  state do
    channel :token, [:@loc]
  end

  bloom :make_ring do
    initial_data <= node do |n|
      # Calculate the successor node
      succ_id = (node_id + 1) % node_count.first.num
      succ_addr = [node[[succ_id]].addr]
      [ n.uid, :next_node, [succ_id, succ_addr] ]
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
    table :next_node, [] => [:node_id, :addr]
    table :token_persist, [:loc]
  end

  bloom :pass_token do
    # Persist the token for as long as necessary
    token_persist <= aft_recv { ip_port }
    token_persist <= token
    token_persist <- (token_persist * next_node).lefts
    # Pass on the token
    aft_send <= (token_persist * next_node).pairs do |tp, nn|
      [(@node_id + 1) % node_count.first.num, 0]
    end
  end

  bloom :print_token do
    stdio <~ token {["#{ip_port}: Got token!"]}
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
