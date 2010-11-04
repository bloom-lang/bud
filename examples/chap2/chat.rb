# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'zlib'
require 'chat_protocol'

class ChatClient < Bud
  def state
    self.extend ChatProtocol
    table :me, ['addr', 'username']
    table :master, ['addr']
    table :status, ['master', 'value']
  end

  declare
  def connect
    me <= [[@ip_port, ARGV[1]]]
    master <= [[ARGV[2]]]
    ctrl <~ join([master,me]).map do |m,me|
      if not status.map{|s| s.master}.include? m.addr
        puts "connecting to #{m.addr}" unless 
        [m.addr, @ip_port, 'join:'+me.username]
      end
      nil
    end
    # change status to live on ack
    status <= join([ctrl,master]).map do |c,m| 
      puts "connected" if m.addr == c.from and c.cmd == 'ack' 
      [m.addr, 'live'] if m.addr == c.from and c.cmd == 'ack'
    end
    term <= status.map{|s| [s.inspect]}
  end
  
  def nice_time
    t = Time.new
    return t.hour.to_s + ":" + t.min.to_s + "." + t.sec.to_s
  end    

  declare
  def chatter
    # send mcast requests to master
    mcast <~ join([term,master,me]) { |t, m, me| [m.addr, @ip_port, me.username, nice_time, t.line] }
    # print mcast msgs from master
    term <= mcast.map {|m| ["("+m.time+") " + m.username + ": " + m.msg]}
  end
end


source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = ChatClient.new(ip, port)
program.run
