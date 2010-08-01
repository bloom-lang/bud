# pingpong demo
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class Ponger < Bud
  attr_reader :myloc
  attr_reader :otherloc

  def initialize(ip, port)
    super ip, port
    dest = ARGV[1].split(':')
    @otherip = dest[0]
    @otherport = dest[1]
    @myloc = ARGV[0]
    @otherloc = ARGV[1]
  end

  def state
    channel :pingpongs, 0, ['otherloc', 'myloc', 'msg', 'wall', 'bud']
  end

  def declaration
    strata[0] = rules {
      # whenever we get a ping, send a pong
      pingpongs <+ pingpongs.map {|p| [@otherloc, @myloc, 'pong!', Time.new.to_s, budtime]}      
    }
  end
end

source = ARGV[0].split(':')
program = Ponger.new(source[0], source[1])
program.run
