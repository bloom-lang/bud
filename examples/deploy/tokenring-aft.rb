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
    scratch :do_init, [] => [:x]
    table :done_init, [] => [:x]
  end

  # The node with ID 0 starts with the token
  bloom :init_token do
    # XXX: use node_ready?
    do_init <= node_count do |nc|
      [true] if @node_id == 0 and done_init.empty?
    end
    done_init <+ do_init
    aft_send <= (do_init * node_count).rights do |nc|
      [(@node_id + 1) % nc.num, 0]
    end
  end

  bloom :pass_token do
    # Pass on the token
    aft_send <= (aft_recv * node_count).pairs do |r, nc|
      puts "Passing on token => #{(@node_id + 1) % nc.num} (recv_msg = #{r.inspect})" if @node_id == 0
      [(@node_id + 1) % nc.num, r.payload + 1]
    end
  end

  bloom :print_token do
    stdio <~ aft_recv {["#{@node_id}: Got token! (@ #{ip_port}, pid = #{Process.pid})"]}
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
