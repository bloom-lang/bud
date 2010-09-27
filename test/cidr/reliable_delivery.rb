require 'rubygems'
require 'bud'

require 'cidr/delivery'

class ReliableDelivery < BestEffortDelivery

  def state
    super
    channel :ack, 0, ['src', 'dst', 'id']
    #table :mem, ['id']
  end
  
  declare
    def more_snd
      #mem <+ pipe.map{|p| [p.id]}
    end

  declare
    def rcv
      ack <+ pipe_chan.map do |p| 
        #print "#{@addy}:#{@budtime} recv pipe_chan (for #{p.dst}).  consider acking\n"
        if p.dst == @addy
          [p.src, p.dst, p.id] 
        end
      end
    end

  declare 
    def done
      j = join [ack, pipe], [ack.id, pipe.id]
      pipe_out <= j.map do |a, p| 
        p
      end
    end
end


