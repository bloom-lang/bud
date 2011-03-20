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

  # send connection request to master on startup
  bootstrap do
    signup <~ [[@master, [ip_port, @nick]]]
  end

  # format chat messages with timestamp on the right of the screen
  def left_right_align(x, y)
    return x + " "*[66 - x.length,2].max + y
  end

  bloom :chatter do
    # send mcast requests to master
    mcast <~ stdio.map do |s|
      [@master, [ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]]
    end
    # pretty-print mcast msgs from master on terminal
    stdio <~ mcast.map do |m|
      [left_right_align(m.val[1].to_s + ": " \
                        + (m.val[3].to_s || ''),
                        "(" + m.val[2].to_s + ")")]
    end
  end
end
