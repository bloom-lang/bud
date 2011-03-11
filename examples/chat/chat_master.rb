# simple chat
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatMaster
  include Bud
  include ChatProtocol

<<<<<<< HEAD
  state { table :nodelist }

  declare
  def master_logic
    nodelist <= signup.payloads
    mcast <~ join([mcast, nodelist]).map do |m,n| 
=======
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
>>>>>>> c62257d60591cf3adaf514299eddf45a5d1b90b8
      [n.key, ip_port, m.nick, m.time, m.msg]  unless n.key == m.from
    end
  end
end
