# pingpong demo #1
# See pinger.rb for instructions.
require 'rubygems'
require 'bud'
require 'ping_protocol'

class Ponger
  include Bud
  include PingProtocol
  
  def initialize(options)
    @otherguy = options[:otherguy]
    super(options)
  end
  
  declare
  def logic
    # whenever we get a ping, send a pong
    flow <~ flow.map {|p| [ARGV[1], ["pong!", Time.new.to_s]]}
    # and output to the screen
    stdio <~ flow.inspected
  end
end

ip, port = ARGV[0].split(':')
ARGV[2] ||= 2
program = Ponger.new(:ip => ip, :port => port, :otherguy => ARGV[1], :delay => ARGV[2])
program.run
