require 'rubygems'
require 'bud'

module DeliveryProtocol
  def state
    super
    channel :pipe_chan, ['@dst', 'src', 'id'], ['payload']
    channel :tickler, ['@self']
  end

  def initialize(host, port, opts = nil)
    @addy = "#{host}:#{port}"
    super(host, port, opts)
  end
end

module BestEffortDelivery
  include DeliveryProtocol
  include Anise
  annotator :declare

  def state
    super
    table :pipe, ['dst', 'src', 'id'], ['payload']
    scratch :pipe_out, ['dst', 'src', 'id'], ['payload']
    periodic :timer, 1
  end
  
  declare
    def snd
      pipe_chan <~ join([pipe, timer]).map do |p, t|
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

  declare 
    def geecee
      pipe <- pipe_out.map{|p| p }  
    end
end


