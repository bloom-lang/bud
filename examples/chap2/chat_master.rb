# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'zlib'
require 'chat_protocol'

class ChatMaster < Bud
  def state
    self.extend ChatProtocol
    table :nodelist, ['addr'], ['username']    
  end
  declare
  def accept
    nodelist <= ctrl.map {|c| [c.addr, c.cmd.split(":")[1]] if c.cmd.split(":")[0] = 'join'}
    ctrl <~ ctrl.map { |c,m| [c.from, @ip_port, 'ack']}
    term <= ctrl.map { |c| [c.inspect] }
  end
  
  def multicast
    mcast <~ join([mcast, nodelist, me]).map do |m,n,me| 
      [n.addr, @ip_port, m.username, m.time, m.msg] unless n.addr == m.from
    end
  end
end


source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = ChatMaster.new(ip, port)
program.run
