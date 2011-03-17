# simple chat
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatMaster
  include Bud
  include ChatProtocol

  state { table :nodelist }

  bloom :master_logic do
    nodelist <= signup.payloads
    mcast <~ join([mcast, nodelist]).map do |m,n| 
      [n.key, ip_port, m.nick, m.time, m.msg]  unless n.key == m.from
    end
  end
end
