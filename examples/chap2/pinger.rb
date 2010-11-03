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
    channel :pingpongs, ['@otherloc', 'myloc', 'msg', 'wall', 'bud']
    periodic :timer, ARGV[2]
  end

  declare
  def logic
    # whenever we get a timer, send out a tuple
    pingpongs <~ timer.map {|t| [@other, @me, 'ping!', t.time, budtime]}      
    pingpongs.each {|p| puts "Got #{p.inspect}"}
  end
end

source = ARGV[0].split(':')
program = Pinger.new(source[0], source[1])
program.run
