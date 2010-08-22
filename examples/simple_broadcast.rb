require 'rubygems'
require 'bud'

class SimpleBroadcast < Bud
  attr_reader :myloc
  attr_reader :otherloc

  def initialize(ip, port)
    print "INIT\n"
    super ip, port
    @myloc = ip.to_s + ":" + port.to_s
  end

  def state
    table :members, ['myloc', 'otherloc']
    table :member, ['otherloc']
    # we expect message to be of a tuple type, for abstraction
    scratch :message, ['message']

    channel :broadcast, 0, ['otherloc', 'myloc', 'msg']
    scratch :deliver, ['message', 'otherloc']
  end

  def declaration
    strata[0] = rules {
      members <= member.map { |m| [@myloc, m.otherloc] } 
      j = join [message, members]
      broadcast <+ j.map { |m, g| [g.otherloc, g.myloc, m.message] } 
      deliver <+ broadcast.map{ |b| [b.msg, b.otherloc] } 
    }
  end
end


