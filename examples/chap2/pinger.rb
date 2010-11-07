# pingpong demo #1
# This demo uses separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class Pinger < Bud
  def initialize(me, other, period)
    @me = me
    @other = other
    @period = period
    ip, port = me.split(':')
    super ip, port
  end
  
  def state
    channel :flow, ['@otherloc', 'me', 'msg', 'wall', 'budtick']
    periodic :timer, @period, ['id'], ['time']
    terminal :out, ['text']
  end

  declare
  def logic
    # whenever we get a timer event, send out a tuple
    flow <~ timer.map {|t| [@other, @me, 'ping!', t.time, budtime]}      
    out <= flow.map {|f| [f.inspect]}
  end
end

program = Pinger.new(ARGV[0], ARGV[1], ARGV[2])
program.run
