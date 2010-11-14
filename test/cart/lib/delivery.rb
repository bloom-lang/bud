require 'rubygems'
require 'bud'

module DeliveryProtocol
  def state
    super
    scratch :pipe_in, ['dst', 'src', 'ident'], ['payload']
    scratch :pipe_out, ['dst', 'src', 'ident'], ['payload']

    channel :pipe_chan, ['@dst', 'src', 'ident'], ['payload']
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


