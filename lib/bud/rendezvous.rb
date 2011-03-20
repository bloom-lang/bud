require 'bud'
require 'msgpack'
require 'eventmachine'
require 'socket'
require 'timeout'

module GlueServer
  attr_accessor :q

  def receive_data(data)
    pac = MessagePack::Unpacker.new
    pac.feed data
    pac.each {|p| q.push p[1][1]}
  end
end

class Rendezvous
  @@stashed_cons = []

  # Ruby-Bud glue.
  def initialize(instance, interface)
    @q = Queue.new
    @conn = 0
    # start up a listener
    @server = EventMachine::open_datagram_socket("localhost", 0, GlueServer) do |con|
      con.q = @q
    end
    @conn = Socket.unpack_sockaddr_in(@server.get_sockname)[0]
    my_addr = "localhost:#{@conn}"
    # install a handler
    instance.sync_do do
      unless instance.tables[:proxy]
        instance.instance_eval {channel :proxy}
      end
    end
    instance.sync_do do
      instance.instance_eval do
        strata << lambda {proxy <~ interface.map{|m| [my_addr, m]} }
      end
    end
    @instance = instance
  end
  
  def block_on(timeout=nil)
    Timeout::timeout(timeout) do
      ret =  @q.pop
      return ret
    end
    raise "Timeout"
  end 

  def stop
    @instance.sync_do do
      @instance.strata.pop
    end
  end
end
