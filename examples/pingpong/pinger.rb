# pingpong demo #1: separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346'
#  you should see messages received on either side
require 'rubygems'
require 'bud'
require 'ping_protocol'

class Pinger
  include Bud
  include PingProtocol
  
  def initialize(options)
    @delay = options[:delay]
    @otherguy = options[:otherguy]
    super(options)
  end 

  state do
    periodic :timer, ARGV[2]
  end

  declare
  def logic
    # whenever we get a timer event, send out a ping
    flow <~ timer.map {|t| [ARGV[1], ['ping!', Time.new.to_s]]}  
    # and output to the screen
    stdio <~ flow.inspected
  end
end

ip, port = ARGV[0].split(':')
ARGV[2] ||= 2
program = Pinger.new(:ip => ip, :port => port, :otherguy => ARGV[1], :delay => ARGV[2])
program.run