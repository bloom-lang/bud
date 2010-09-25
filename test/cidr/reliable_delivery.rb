require 'rubygems'
require 'bud'

require 'cidr/delivery'

class ReliableDelivery < BestEffortDelivery

  def state
    super
    channel :ack, 0, ['peer', 'self', 'id']
    table :mem, ['id']
  end
  
  declare
    def more_snd
      mem <+ pipe.map{|p| [p.id]}
    end

  declare
    def rcv
      ack <+ pipe_chan.map do |p| 
        if p.peer == @addy
          print "delivered!  acking from #{@port}\n"
          [p.self, p.peer, p.id] 
        end
      end
    end

  declare 
    def done
      ack.map{|a| print "got ack!! at #{@port}\n"}
      j = join [ack, pipe], [ack.id, pipe.id]
      pipe_out <= j.map{|a, p| p}
    end
end


