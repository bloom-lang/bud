class Bud
  ######## the EventMachine server for handling network and timers
  class Server < EM::Connection
    attr_accessor :bud

    def initialize(*args)
      @pac = MessagePack::Unpacker.new
      super
    rescue Exception
      print "An error occurred initializing BudServer: ",$!, "\n"
    end

    def post_init
      @port, @ip = Socket.unpack_sockaddr_in(get_peername)
      puts "-- server inbound connection from #{@ip}:#{@port}"
      $connections = {} if $connections.nil?
      $connections[[@ip, @port]] = self
    rescue Exception
      print "An error occurred post_init on BudServer: ",$!, "\n"
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
      puts "got " + obj.inspect
      if (obj.class <= Array and obj.length == 2 and not bud.tables[obj[0].to_sym].nil? and obj[1].class <= Array) then
        $inbound << obj
        bud.tick
      else
        puts " ... BAD!"
        bud.tick
      end
    end

    def unbind
      puts "-- connection ended from #{@ip}:#{@port}"
      $connections.delete [@ip,@port]
    end
  end
end
