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
    table :nodelist, ['addr'], ['nick']    
  end
  
  declare
  def accept
    nodelist <= ctrl.map {|c| [c.from, c.cmd] }
    ctrl <~ ctrl.map { |c| [c.from, ip_port, 'ack']}
  end
  
  declare
  def multicast
    mcast <~ join([mcast, nodelist]).map do |m,n| 
      [n.addr, ip_port, m.nick, m.val, m.msg]  unless n.addr == m.from
    end
  end
end
