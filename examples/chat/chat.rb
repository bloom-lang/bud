# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatClient
  include Bud
  include ChatProtocol

  def initialize(me, master, opts)
    @me = me
    @master = master
    # @port = opts[:port]
    # @ip = opts[:ip]
    super opts
  end

  state do
    table :status
  end

  bootstrap do
    # send connection request to master
    ctrl <~ [[@master, ip_port, @me]]
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
     status <= ctrl do |c|
       if @master == c.from and c.cmd == 'ack'
         [@master, 'live']
       end
     end
     stdio <~ ctrl.inspected

    # send mcast requests to master if status is non-empty
    mcast <~ join([stdio, status]) do |t,s|
      [@master, ip_port, @me, nice_time, t.line]
    end
    # pretty-print mcast msgs from master on terminal
    stdio <~ mcast do |m|
      [left_right_align(m.nick + ": " \
                        + (m.msg || ''),
                        "(" + m.time + ")")]
    end
  end
end
