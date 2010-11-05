# pingpong demo #1
# This demo uses separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class Pinger < Bud
  def initialize(ip, port)
    super ip, port
    @me = ARGV[0]
    @other = ARGV[1]
  end
  
  def state
    channel :flow, ['@otherloc', 'me', 'msg', 'wall', 'budtick']
    periodic :timer, ARGV[2], ['id'], ['time']
    terminal :out, ['text']
  end

  declare
  def logic
    # whenever we get a timer, send out a tuple
    flow <~ timer.map {|t| [@other, @me, 'ping!', t.time, budtime]}      
    out <= flow.map {|f| [f.inspect]}
  end
end

source = ARGV[0].split(':')
program = Pinger.new(source[0], source[1])
program.run


