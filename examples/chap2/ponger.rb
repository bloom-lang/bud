# pingpong demo
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up a pinger with 'ruby pinger.rb 127.0.0.1:12345 127.0.0.1:12346 2'
#  you should see packets received on either side
require 'rubygems'
require 'bud'
require 'ping_protocol'

class Ponger < Bud
  include PingProtocol

  def initialize(me, other)
    @me = me
    @other = other
    ip, port = me.split(':')
    super ip, port
  end

  def state
    ping_protocol_state
  end

  declare
  def logic
    # whenever we get a ping, send a pong
    flow <~ flow.map {|p| [@other, @me, p.msg+": pong!", Time.new.to_s, budtime]}
    out <= flow.map {|f| [f.inspect]}
  end
end

program = Ponger.new(ARGV[0], ARGV[1])
program.run
