require 'rubygems'
require 'bud'
require 'lib/reliable_delivery'
require 'lib/queue'
#require 'lib/kvs'
require 'lib/kvs_pedagogical'


# extend the key-value store in such a way that it has a (arguably) reasonable
# behavior when presented with multiple puts for the same key in the same
# timestep.

module KVSMetering
  include BaseQueue
  include KVSProtocol
  include Anise
  annotator :declare

  def state
    super
    scratch :meter_can_store, ['ident'], ['payload']
  end

  # if you don't write indir yourself (to compose stuff), we'll do it
  # for you:
  declare
  def indir
    can_store <= meter_can_store.map {|c| c}
  end

  declare
  def queueinglogic
    q <= kvstore.map do |k| 
      [k.reqid, k] 
    end

    meter_can_store <= head.map do |h| 
      print @budtime.to_s + " Indirecting: "+ h.payload.inspect + "\n" or h 
    end
    consumed <+ head.map{|h| [h.ident] } 
  end
end
