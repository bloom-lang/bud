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
    
    #interface output, :pipe_sent, ['dst', 'src', 'ident'], ['payload']
    periodic :tock, 10
  end
  
  declare 
  def remember
    #pipe <= deliveryprotocol_pipe_in.map {|p| p }
    pipe <= pipe_in.map {|p| puts "store pipe" or p }

    # this should be automatic
    #pipe_sent <= pipe_sent{|p| puts "GACK GACK" or p }

    pipe_chan <~ join([pipe, tock]).map{|p, t| puts "resend" or p }
  end
  
  declare
  def rcv
    #ack <~ deliveryprotocol_pipe_chan.map {|p| [p.src, p.dst, p.ident] }
    ack <~ pipe_chan.map {|p| puts @addy +  " ack" or [p.src, p.dst, p.ident] }
  end

  declare 
  def done 
    apj = join [ack, pipe], [ack.ident, pipe.ident]
    #pipe_sent <= join([ack, pipe], [ack.ident, pipe.ident]).map {|a, p| p }
    pipe_sent <= apj.map {|a, p| puts "apj" or p }
    #pipe <- apj.map {|a, p| p }
  end
end


