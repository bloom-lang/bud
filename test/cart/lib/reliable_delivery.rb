require 'rubygems'
require 'bud'

require 'lib/delivery'

#class ReliableDelivery < BestEffortDelivery
module ReliableDelivery
  include  BestEffortDelivery
  include Anise
  annotator :declare

  def state
    super
    channel :ack, ['@src', 'dst', 'id']
  end
  
  declare
    def rcv
      ack <~ pipe_chan.map do |p| 
        if p.dst == @addy
          [p.src, p.dst, p.id] 
        end
      end
    end

  declare 
    def done 
      pipe_out <= join([ack, pipe], [ack.id, pipe.id]).map do |a, p| 
        p
      end
    end
end


