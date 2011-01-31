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

  declare
  def logic
    # whenever we get a ping, send a pong
    flow <~ flow.map {|p| [ARGV[1], ARGV[0], p.msg+": pong!", Time.new.to_s, budtime]}
    stdio <~ flow.map {|f| [f.inspect]}
  end
end

ip, port = ARGV[0].split(":")
program = Ponger.new(:ip => ip, :port => port)
program.run
