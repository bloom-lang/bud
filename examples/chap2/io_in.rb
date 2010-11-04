# pingpong demo #1
# This demo uses separate scripts for pinger and ponger.
# To run:
#  fire up a ponger with 'ruby ponger.rb 127.0.0.1:12346 127.0.0.1:12345'
#  fire up an io_in with 'ruby io_in.rb 127.0.0.1:12345 127.0.0.1:12346'
#  you should see packets received on either side
require 'rubygems'
require 'bud'

class IoIn < Bud
  def initialize(ip, port)
    super ip, port
    @me = ARGV[0]
    @other = ARGV[1]
  end

  def state
    channel :pingpongs, ['@otherloc', 'myloc', 'msg', 'wall', 'bud']
    terminal :interm, ['text']
    terminal :outterm
  end

  declare
  def logic
    # whenever we get a line, send out a tuple
    pingpongs <~ interm.map {|t| [@other, @me, t.text, 0, budtime]}      
    outterm <= pingpongs
  end
end

source = ARGV[0].split(':')
program = IoIn.new(source[0], source[1])
program.run
