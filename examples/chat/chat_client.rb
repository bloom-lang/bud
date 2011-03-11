# simple chat
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatClient
  include Bud
  include ChatProtocol

  def initialize(nick, master, opts)
    @nick = nick
    @master = master
    super opts
  end

  state { table :status }

  # XXX It's kludgy to require clients to conjure up their own port.
  # But bootstrap will fail on a dynamic port.
  # ??

  # send connection request to master
  bootstrap do
    signup <~ [[@master, [ip_port, @nick]]]
  end

  # formatting received chat messages on the right of the screen
  def left_right_align(x, y)
    return x + " "*[66 - x.length,2].max + y
  end

  declare
  def chatter
    # send mcast requests to master if status is non-empty
    mcast <~ stdio.map do |s|
      [@master, ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]
    end
    # pretty-print mcast msgs from master on terminal
    stdio <~ mcast.map do |m|
      [left_right_align(m.nick + ": " \
                        + (m.msg || ''),
                        "(" + m.time + ")")]
    end
  end
end
