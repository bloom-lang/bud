# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'lib/chat_protocol'
require 'lib/2pc'

class ChatMaster < Bud
  include ChatProtocol

  def state
    chat_protocol_state
    table :nodelist, ['addr'], ['nick']    
  end
  
  declare
  def accept
    nodelist <= ctrl.map {|c| [c.from, c.cmd] }
    ctrl <~ ctrl.map { |c| [c.from, @ip_port, 'ack']}
  end
  
  declare
  def multicast
    mcast <~ join([mcast, nodelist]).map do |m,n| 
      [n.addr, @ip_port, m.nick, m.time, m.msg]  unless n.addr == m.from
    end
  end
end

class GracefulStopChatMaster < ChatMaster
  def initialize(i, p, o)
    super(i, p, o)
    @twopc = TwoPCMaster.new(i, p + 100, o)
    @twopc.run_bg
  end

  def state
    super
    scratch :shutdown_req, ['requestid']
    scratch :empty_echo, ['requestid']
  end

  declare 
  def shutdown
    @twopc.request_commit <= shutdown_req.map{|s| print "SHUTDOWN\n" or [s.requestid, "shutdown"] }

    empty_echo <= @twopc.xact.map do |x| 
      if x.status == "Y" 
        raise "Cleanly exit?\n" 
      end
    end
  end
end


