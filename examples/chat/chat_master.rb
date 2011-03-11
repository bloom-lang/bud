# simple chat
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatMaster
  include Bud
  include ChatProtocol

  state { table :nodelist }

  declare
  def master_logic
    nodelist <= signup.payloads
    mcast <~ join([mcast, nodelist]).map do |m,n| 
      [n.key, ip_port, m.nick, m.time, m.msg]  unless n.key == m.from
    end
  end
end
