require 'rubygems'
require 'bud'
require 'lib/delivery'

module ReliableDelivery
  include  BestEffortDelivery
  include Anise
  annotator :declare

  def state
    super
    table :pipe, ['dst', 'src', 'ident'], ['payload']
    channel :ack, ['@src', 'dst', 'ident']
    periodic :tock, 10
  end
  
  declare 
  def remember
    pipe <= pipe_in
    pipe_chan <~ join([pipe, tock]).map{|p, t| p }
  end
  
  declare
  def rcv
    #ack <~ deliveryprotocol_pipe_chan.map {|p| [p.src, p.dst, p.ident] }
    ack <~ pipe_chan.map {|p| puts @addy +  " ack" or [p.src, p.dst, p.ident] }
  end

  declare 
  def done 
    apj = join [ack, pipe], [ack.ident, pipe.ident]
    pipe_sent <= apj.map {|a, p| p }
    pipe <- apj.map {|a, p| p }
  end
end


