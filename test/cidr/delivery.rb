require 'rubygems'
require 'bud'

class BestEffortDelivery < Bud

  def initialize(host, port)
    @addy = "#{host}:#{port}"
    super(host, port)
  end

  def state
    table :pipe, ['peer', 'self', 'id'], ['payload']
    table :pipe_out, ['peer', 'self', 'id'], ['payload']
    channel :pipe_chan, 0, ['peer', 'self', 'id'], ['payload']
    channel :tickler, 0, ['self']
  end
  
  declare
    def snd
      pipe_chan <+ pipe.map do |p| 
        unless pipe_out.map{|m| m.id}.include? p.id
          #print "OK, got pipe #{p.inspect}\n"; 
          p 
        end
      end
    end

  declare
    def rcv
      # do something.
    end

  declare 
    def done
      # vacuous ackuous.  override me!
      pipe_out <+ pipe.map{|p| p}
    end
end


