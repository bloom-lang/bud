require 'rubygems'
require 'bud'

module DeliveryProtocol
  def state
    super
    interface input, :pipe_in, ['dst', 'src', 'ident'], ['payload']
    interface output, :pipe_sent, ['dst', 'src', 'ident'], ['payload']
    channel :tickler, ['@myself']
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
    # PAA -- note that something is broken about the new @ syntax.  downstream modules referencing 'dst' 
    # when it is prefixed with '@' in its declaration get errors.  temporary fix is to undo the '@'.
    #channel :pipe_chan, ['@dst', 'src', 'ident'], ['payload']
    channel :pipe_chan, ['dst', 'src', 'ident'], ['payload']
  end

  declare
    def snd
      pipe_chan <~ pipe_in
    end

  declare 
    def done
      # vacuous ackuous.  override me!
      pipe_sent <= pipe_in
    end
end


