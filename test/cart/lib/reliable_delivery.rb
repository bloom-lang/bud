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
  end
  
  declare 
  def remember
    pipe <= pipe_in.map {|p| p }
  end
  
  declare
  def rcv
    ack <~ pipe_chan.map {|p| [p.src, p.dst, p.ident] }
  end

  declare 
  def done 
    pipe_out <= join([ack, pipe], [ack.ident, pipe.ident]).map {|a, p| p }
  end
end


