# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatClient < Bud
  include ChatProtocol
  def initialize(ip, port, me, master)
    super ip, port
    @me = me
    @master = master
  end
  
  def state
    chat_protocol_state
    table :status, ['master', 'value']
  end

  declare
  def connect
    # if we haven't contacted master, do so now
    ctrl <~ [[@master, @ip_port, 'join:'+@me]] unless status.map{|s| s.master}.include? @master

    # add "live" status on ack
    status <= ctrl.map {|c| [@master, 'live'] if @master == c.from and c.cmd == 'ack'}
  end
  
  def nice_time; return Time.new.strftime("%I:%M.%S"); end   
  
  def left_right_align(x, y); return x + " "*[66 - x.length,2].max + y;  end
  
  declare
  def chatter
    # send mcast requests to master if status is non-empty
    mcast <~ join([term, status]).map { |t,s| [@master, @ip_port, @me, nice_time, t.line] }
    # pretty-print mcast msgs from master on terminal
    term <= mcast.map do |m|
      [left_right_align(m.username + ": " + (m.msg || ''), "(" + m.time + ")")]
    end
  end
end


source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = ChatClient.new(ip, port, ARGV[1], ARGV[2])
program.run
