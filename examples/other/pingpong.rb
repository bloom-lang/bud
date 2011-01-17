# pingpong demo
# This script unifies pinger and ponger into a single program.
# To run:
#  fire up one copy with 'ruby pingpong.rb 12345 2 true'
#  fire up a second copy with 'ruby pingpong.rb 12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class PingPong < Bud
  def initialize(ip, port)
    super ip, port
    @myloc = ip.to_s + ":" + port.to_s
    loc1 = "127.0.0.1:12345"
    loc2 = "127.0.0.1:12346"
    @otherloc = (@myloc == loc1) ? loc2 : loc1
  end

  def state
    channel :pipe, ['@otherloc', 'myloc', 'msg', 'wall', 'bud']
    table   :pingbuf, ['otherloc', 'myloc', 'msg', 'wall', 'bud']
    periodic :timer, ARGV[1]
  end

  def bootstrap
    if ARGV[2]
      # XXX: fix this
      #stdio <~ [["injecting into pingbuf"]]
      pingbuf << [@otherloc, @myloc, 'pong!', Time.new.to_s, budtime]
    end
  end

  declare
  def rules
    # whenever we get a message, store it in pingbuf and print it
    pingbuf <= pipe
    stdio <~ pipe.map {|p| ["got message: " + p.msg]}

    # whenever we get a timer event, send out the contents of pingbuf, and
    # delete them for the next tick
    j = join [timer, pingbuf]
    pipe <~ j.map {|t,p| [@otherloc, @myloc, (p.msg == 'ping!') ? 'pong!' : 'ping!', t.time, budtime]}
    pingbuf <- j.map {|t,p| [p.otherloc, p.myloc, p.msg, p.wall, p.bud]}
  end
end

program = PingPong.new('127.0.0.1', ARGV[0])
program.run
