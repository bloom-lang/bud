require 'rubygems'
require 'bud'

require 'lib/simple_broadcast'

class ReliableBroadcast < SimpleBroadcast
  attr_reader :myloc
  attr_reader :otherloc

  def initialize(ip, port)
    super ip, port
  end

  def state
    # ideally this would 'just happen' as part of inheritance...
    super
    scratch :rmessage, ['message']
    scratch :rdeliver, ['message', 'sender']
    scratch :to_deliver, ['message', 'sender']
    table :delivered, ['message']
  end

  def declaration
    super
    
    # jacked up: pick a stratum that won't clobber those instantiated by our superclass
    strata[1] = rules {
      # enqueue the messages via simple broadcast
      message <+ rmessage.map {|rm| [rm.message] }
      to_deliver <= deliver.map{|d| [d.message] unless delivered.map{|rd| rd.message}.include? d.message}
  
    }
    strata[2] = rules {
      # if we get a message delivered, broadcast it!
      message <+ to_deliver.map{|m| [m.message]}
      # and then locally deliver it. 
      rdeliver <+ to_deliver.map{|m| m}
      delivered <= to_deliver.map{|m| [m.message]}
    }
  end
end

