# pingpong demo #1
# This demo uses separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'
require 'ping_protocol'

class Pinger < Bud
  include PingProtocol

  def state
    super
    periodic :timer, ARGV[2]
  end

  declare
  def logic
    # whenever we get a timer event, send out a tuple
    flow <~ timer.map {|t| [ARGV[1], ARGV[0], 'ping!', t.time, budtime]}      
    stdio <~ flow.map {|f| [f.inspect]}
  end
end

ip, port = ARGV[0].split(":")
program = Pinger.new(:ip => ip, :port => port)
program.run
