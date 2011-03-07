# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatMaster
  include Bud
  include ChatProtocol

  state do
    table :nodelist
  end
  
  declare
  def accept
    nodelist <= ctrl {|c| [c.from, c.cmd]}
    ctrl <~ ctrl { |c| [c.from, ip_port, 'ack']}
    stdio <~ ctrl.inspected
  end
  
  declare
  def multicast
    mcast <~ join([mcast, nodelist]) do |m,n| 
      [n.key, ip_port, m.nick, m.time, m.msg]  unless n.key == m.from
    end
  end
end
