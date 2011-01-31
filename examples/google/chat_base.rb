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
  def initialize(me, master, opts)
    @me = me
    @master = master
    super opts
  end
  
  def state
    super if defined? super
    table :status, ['master', 'value']
  end
  
  def bootstrap
    # send connection request to master
    ctrl <~ [[@master, @ip_port, @me]]
  end

  def nice_time
    return Time.new.strftime("%I:%M.%S")
  end   
  
  def left_right_align(x, y)
    return x + " "*[66 - x.length,2].max + y
  end
  
  declare
  def chatter
    # add "live" status on ack
     status <= ctrl.map do |c|
       if @master == c.from and c.cmd == 'ack'
         [@master, 'live'] 
       end
     end
 
    # send mcast requests to master if status is non-empty
    mcast <~ join([stdio, status]).map do |t,s| 
      [@master, @ip_port, @me, nice_time, t.line]
    end
    # pretty-print mcast msgs from master on terminal
    stdio <~ mcast.map do |m|
      [left_right_align(m.nick + ": " \
                        + (m.msg || ''), 
                        "(" + m.time + ")")]
    end
  end
end

