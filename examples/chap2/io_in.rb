# pingpong demo #1
# This demo uses separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up an io_in with 'ruby io_in.rb 127.0.0.1:12345 127.0.0.1:12346'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class IoIn < Bud
  def initialize(opts)
    super
    @me = ARGV[0]
    @other = ARGV[1]
  end

  def state
    channel :flow, ['@otherloc', 'myloc', 'msg', 'wall', 'budtick']
  end

  declare
  def logic
    # whenever we get a line, send out a tuple
    flow <~ stdio.map {|t| [@other, @me, t.line, 0, budtime]}      
    stdio <~ flow.map {|f| [f.inspect]}
  end
end

raise "usage: io_in my_ip:port remote_ip:port" if ARGV.length != 2
source = ARGV[0].split(':')
program = IoIn.new(:ip => source[0], :port => source[1])
program.run
