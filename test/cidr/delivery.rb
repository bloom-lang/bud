require 'rubygems'
require 'bud'

class BestEffortDelivery < Bud

  def initialize(host, port)
    @addy = "#{host}:#{port}"
    super(host, port)
  end

  def state
    table :pipe, ['dst', 'src', 'id', 'payload']
    table :pipe_out, ['dst', 'src', 'id'], ['payload']
    channel :pipe_chan, 0, ['dst', 'src', 'id'], ['payload']
    channel :tickler, 0, ['self']
    periodic :timer, 1
  end
  
  declare
    def snd
      pipe_chan <+ join([pipe, timer]).map do |p, t|
        unless pipe_out.map{|m| m.id}.include? p.id
          p 
        end
      end
    end

  declare 
    def done
      # vacuous ackuous.  override me!
      pipe_out <+ join([pipe, timer]).map do |p, t| 
        p
      end
    end
end


