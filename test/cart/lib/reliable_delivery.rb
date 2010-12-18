require 'rubygems'
require 'bud'
require 'lib/delivery'

module ReliableDelivery
  include  BestEffortDelivery
  include Anise
  annotator :declare

  def state
    super
    print "RD state\n"
    table :pipe, ['dst', 'src', 'ident'], ['payload']
    channel :ack, ['@src', 'dst', 'ident']
    internal output, :pipe_out
    internal input, :pipe_in
    periodic :tock, 10
  end
  
  declare 
  def remember
    pipe <= pipe_in.map {|p| p }
    pipe_chan <~ join([pipe, tock]).map{|p, t| p }
  end
  
  declare
  def rcv
    ack <~ pipe_chan.map {|p| [p.src, p.dst, p.ident] }
  end

  declare 
  def done 
    apj = join [ack, pipe], [ack.ident, pipe.ident]
    #pipe_out <= join([ack, pipe], [ack.ident, pipe.ident]).map {|a, p| p }
    pipe_out <= apj.map {|a, p| p }
    pipe <- apj.map {|a, p| p }
  end
end


