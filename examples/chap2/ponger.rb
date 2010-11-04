# pingpong demo
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class Ponger < Bud
  def initialize(ip, port)
    super ip, port
    @me = ARGV[0]
    @other = ARGV[1]
  end

  def state
    channel :flow, ['@otherloc', 'me', 'msg', 'wall', 'budtick']
    terminal :out, ['text']
  end

  declare
  def logic
    # whenever we get a ping, send a pong
    out <= flow
    flow <~ flow.map {|p| [@other, @me, p.msg+": pong!", Time.new.to_s, budtime]}      
  end
end

source = ARGV[0].split(':')
program = Ponger.new(source[0], source[1])
program.run
