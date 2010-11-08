# simple chat.  run from the directory above as follows:
# run "ruby lib/chat_master_inheritance.rb 127.0.0.1:12345"
# run "ruby lib/chat_inheritance.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby lib/chat_inheritance.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby lib/chat_inheritance.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'lib/chat_protocol'
require 'lib/2pc'

class ChatMaster < TwoPCMaster
  include ChatProtocol

  def state
    super
    chat_protocol_state
    table :nodelist, ['addr'], ['nick']    
    scratch :empty_echo, ['xid']
  end
  
  declare
  def accept
    nodelist <= ctrl.map {|c| [c.from, c.cmd] }
    member <= ctrl.map {|c| [c.from] }
    ctrl <~ ctrl.map { |c| [c.from, @ip_port, 'ack']}
  end
  
  declare
  def multicast
    mcast <~ join([mcast, nodelist]).map do |m,n| 
      [n.addr, @ip_port, m.nick, m.time, m.msg]  unless n.addr == m.from
    end
  end

  declare
  def shutdown
    empty_echo <= xact.map do |x|
      if x.status == "commit" then
        exit
#        raise "Cleanly exit? #{x.data}"
      end
    end
  end
end

#source = ARGV[0].split(':')
#ip = source[0]
#port = source[1].to_i
#program = ChatMaster.new(ip, port, {'visualize' => false, 'dump' => true})

#program.run_bg
#sleep 10
#program.request_commit <+ [[ 123, 'shutdown' ]] 

#sleep 60
