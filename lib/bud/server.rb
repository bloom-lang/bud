class Bud
  ######## the EventMachine server for handling network and timers
  class BudServer < EM::Connection
    attr_accessor :bud

    def initialize(thebud)
      @pac = MessagePack::Unpacker.new
      @bud = thebud
      super
    rescue Exception
      puts "An error occurred initializing BudServer: #{$!}"
    end

    def post_init
      pname = get_peername
      if pname
        @port, @ip = Socket.unpack_sockaddr_in(pname) 
 #       puts "-- server inbound connection from #{@ip}:#{@port}"
      else
        @port, @ip = Socket.unpack_sockaddr_in(get_sockname)
 #       puts "-- server connection to #{@ip}:#{@port}"
      end
      bud.connections[[@ip, @port]] = self
    rescue Exception
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
#      puts "got " + obj.inspect
      if (obj.class <= Array and obj.length == 2 and not bud.tables[obj[0].to_sym].nil? and obj[1].class <= Array)
        bud.inbound << obj
        bud.tick
      else
        raise BudError, " ... bad inbound message of class #{obj.class}: #{obj.inspect}!"
        # bud.tick
      end
    end

    def unbind
#      puts "-- connection ended from #{@ip}:#{@port}"
      bud.connections.delete [@ip, @port]
    end
  end
end
