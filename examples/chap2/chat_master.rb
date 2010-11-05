# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'zlib'
require 'chat_protocol'

class ChatMaster < Bud
  include ChatProtocol
  def state
    channel :mcast, ['@to', 'from', 'username', 'time'], ['msg']
    channel :ctrl, ['@to', 'from', 'cmd']
    terminal :term
    table :nodelist, ['addr'], ['username']    
  end
  declare
  def accept
#    term <= ctrl.map { |c| [c.inspect] }
    nodelist <= ctrl.map {|c| [c.from, c.cmd.split(":")[1]] }
#    term <= nodelist.map {|n| [n.inspect]}
    ctrl <~ ctrl.map { |c| [c.from, @ip_port, 'ack']}
  end
  
  declare
  def multicast
    mcast <~ join([mcast, nodelist]).map do |m,n| 
#      puts "joining mcast with nodelist"
      [n.addr, @ip_port, m.username, m.time, m.msg]  unless n.addr == m.from
    end
    term <= join([mcast, nodelist]).map do |m,n| 
      [n.addr, @ip_port, m.username, m.time, m.msg] unless n.addr == m.from
    end
  end
end


source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = ChatMaster.new(ip, port)
program.run
