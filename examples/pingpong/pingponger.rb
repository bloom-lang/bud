# pingpong demo #2: peer-to-peer pingpong.
# To run:
#  fire up one peer with 'ruby pingponger.rb localhost:12346 localhost:12345'
#  fire up another peer with 'ruby pingponger.rb localhost:12345 localhost:12346'
#  you should see messages received on either side
require 'rubygems'
require 'bud'
require 'ping_protocol'

class PingPonger
  include Bud
  
  def initialize(options)
    @delay = options[:delay]
    @otherguy = options[:otherguy]
    super(options)
  end

  state do
    channel :flow
    table :buf
    periodic :timer, @delay
  end
  
  bootstrap do
    buf <+ [["go"]]
  end

  declare
  def logic    
    # trace what we receive
    stdio <~ flow.inspected
    # and remember received payload until it's time to respond
    buf <= flow.payloads

    # when timer goes off and buf is non-empty, this collection has one element
    # for each element of buf.
    coincidence = coincide([timer, buf])
    
    # enqueue responses to elements of buf
    flow <~ coincidence.map do |t,b|
      [@otherguy, (b.key == 'ping!' ? ['pong!', Time.new.to_s] : ['ping!', Time.new.to_s])]
    end
    # and schedule buf to be emptied after this tick
    buf <- coincidence.map {|t,b| b}
  end
end

ip, port = ARGV[0].split(':')
ARGV[2] ||= 2
program = PingPonger.new(:ip => ip, :port => port, :otherguy => ARGV[1], :delay => ARGV[2])
program.run
