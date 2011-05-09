require 'rubygems'
require 'bud'

module TokenRingAft
  bloom :make_ring do
    initial_data <= (node * node_count).pairs do |n, nc|
      [n.uid, :node_count, [[nc.num]]]
    end
  end
end

module TokenRingAftChild
  state do
    table :token_persist, [] => [:x]
    scratch :do_init, [] => [:x]
    table :done_init, [] => [:x]
  end

  # The node with ID 0 starts with the token
  bloom :init_token do
    do_init <= node_count do |nc|
      [true] if @node_id == 0 and done_init.empty?
    end
    done_init <+ do_init
    aft_send <= (do_init * node_count).rights do |nc|
      [(@node_id + 1) % nc.num, 0]
    end
  end

  bloom :pass_token do
    # Persist the token for as long as necessary
    token_persist <= aft_recv { [true] }
    token_persist <- (token_persist * node_count).lefts
    # Pass on the token
    aft_send <= (token_persist * node_count).pairs do |tp, nc|
      [(@node_id + 1) % nc.num, 0]
    end
  end

  bloom :print_token do
#    stdio <~ (token_persist * node_count).pairs {["#{ip_port}: Got token!"]}
  end
end

class RingAft
  include Bud
  include TokenRingAft
  include AftDeploy

  def initialize(opts={})
    super
    @child_modules << TokenRingAftChild
  end

  deploystrap do
    node_count << [10]
  end
end

RingAft.new(:deploy => true).run_fg
