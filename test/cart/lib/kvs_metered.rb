require 'rubygems'
require 'bud'
require 'lib/reliable_delivery'
require 'lib/queue'

# extend the key-value store in such a way that it has a (arguably) reasonable
# behavior when presented with multiple puts for the same key in the same
# timestep.

class MeteredKVS < BudKVS
  def initialize(ip, port)
    @q = BaseQueue.new(ip, port + 1)
    @q.tick
    super(ip, port)
  end

  def tick
    print "TICK\n"
    @q.tick
    super
  end

  def state
    super
  end

  def interpose
    @q.q <= kvstore.map do |k| 
      print "enqueue #{k}\n" or [k.reqid, k] 
    end

    print "Q siz is " + @q.q.length.to_s + "\n"
    kvstore_indirected <= @q.head.map do |h| 
      print "HEAD: #{h.payload}\n" or h.payload 
    end
    @q.consumed <+ @q.head.map{|h| [h.id] } 
  end
end
