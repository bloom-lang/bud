# pingpong demo
# This script unifies pinger and ponger into a single program.
# To run:
#  fire up one copy with 'ruby pingpong.rb 12345 2 true'
#  fire up a second copy with 'ruby pingpong.rb 12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class PingPong
  include Bud

  def initialize(opt)
    super
    @myloc = "#{opt[:ip]}:#{opt[:port]}"
    loc1 = "127.0.0.1:12345"
    loc2 = "127.0.0.1:12346"
    @otherloc = (@myloc == loc1) ? loc2 : loc1
  end

  state {
    channel :pipe, [:@otherloc, :myloc, :msg, :wall, :bud]
    table   :pingbuf, [:otherloc, :myloc, :msg, :wall, :bud]
    periodic :timer, ARGV[1]
  }

  bootstrap do
    if ARGV[2]
      stdio <~ [["injecting into pingbuf"]]
      pingbuf << [@otherloc, @myloc, 'pong!', Time.new.to_s, budtime]
    end
  end

  declare
  def rules
    # whenever we get a message, store it in pingbuf and print it
    pingbuf <= pipe
    stdio <~ pipe.map {|p| ["got message: #{p.msg}"]}

    # whenever we get a timer event, send out the contents of pingbuf, and
    # delete them for the next tick
    j = join [timer, pingbuf]
    pipe <~ j.map {|t,p| [@otherloc, @myloc, (p.msg == 'ping!') ? 'pong!' : 'ping!', t.val, budtime]}
    pingbuf <- j.map {|t,p| [p.otherloc, p.myloc, p.msg, p.wall, p.bud]}
  end
end

program = PingPong.new(:ip => "127.0.0.1", :port => ARGV[0])
program.run
