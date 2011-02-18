require 'socket'

module Bud
  ######## the EventMachine server for handling network and timers
  class BudServer < EM::Connection
    def initialize(bud)
      @pac = MessagePack::Unpacker.new
      @bud = bud
      super
    rescue
      puts "An error occurred initializing BudServer: #{$!}"
    end

    def post_init
      pname = get_peername
      if pname
        @port, @ip = Socket.unpack_sockaddr_in(pname) 
#        puts "-- server inbound connection from #{@ip}:#{@port}"
      else
        @port, @ip = Socket.unpack_sockaddr_in(get_sockname)
#        puts "-- server connection to #{@ip}:#{@port}"
      end
      @bud.connections[[@ip, @port]] = self
    rescue
      puts "An error occurred post_init on BudServer: #{$!}"
    end

    def receive_data(data)
      # Feed the received data to the deserializer
      @pac.feed data

      # streaming deserialize
      @pac.each do |obj|
        message_received(obj)
      end
    end

    def message_received(obj)
#      puts "#{@bud.ip_port} <= #{obj.inspect}"
      unless (obj.class <= Array and obj.length == 2 and not @bud.tables[obj[0].to_sym].nil? and obj[1].class <= Array)
        raise BudError, "Bad inbound message of class #{obj.class}: #{obj.inspect}"
      end

      @bud.inbound << obj
      begin
        @bud.tick
      rescue Exception
        # If we raise an exception here, EM dies, which causes problems (e.g.,
        # other Bud instances in the same process will crash). Ignoring the
        # error isn't best though -- we should do better (#74).
        puts "Exception handling network message (channel '#{obj[0]}'): #{$!}"
      end
    end

    def unbind
#      puts "-- connection ended from #{@ip}:#{@port}"
      @bud.connections.delete [@ip, @port]
    end
  end
end
