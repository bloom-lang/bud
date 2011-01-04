require 'rubygems'
require 'bud'

module DeliveryProtocol
  def state
    #accounting("DeliveryProtocol", self)
    super
    interface input, :pipe_in, ['dst', 'src', 'ident'], ['payload']
    interface output, :pipe_out, ['dst', 'src', 'ident'], ['payload']
    #channel :tickler, ['@self']
  end

  def initialize(host, port, opts)
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
    channel :pipe_chan, ['@dst', 'src', 'ident'], ['payload']
  end

  declare
    def snd
      pipe_chan <~ pipe_in.map{|p| p }
    end

  declare 
    def done
      # vacuous ackuous.  override me!
      pipe_out <= pipe_in.map{|p| p }
    end
end


