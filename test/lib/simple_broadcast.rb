require 'rubygems'
require 'bud'

class SimpleBroadcast < Bud
  attr_reader :myloc
  attr_reader :otherloc

  def initialize(ip, port)
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

  # I expect my subclasses to possibly override the message and deliver logic
  declare
  def p_message
    j = join [message, members]
    broadcast <+ j.map { |m, g| [g.otherloc, g.myloc, m.message] } 
  end

  declare
  def p_deliver
      deliver <+ broadcast.map{ |b| [b.msg, b.myloc] } 
  end

  declare
  def rest
      members <= member.map { |m| [@myloc, m.otherloc] } 
  end
end


