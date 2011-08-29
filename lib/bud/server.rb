require 'socket'

module Bud
  class BudServer < EM::Connection #:nodoc: all
    def initialize(bud)
      @bud = bud
      @pac = MessagePack::Unpacker.new
      super
    end

    def receive_data(data)
      # Feed the received data to the deserializer
      @pac.feed data

      # streaming deserialize
      @pac.each do |obj|
        message_received(obj)
      end

      begin
        @bud.tick_internal unless (@bud.lazy or not @bud.running_async)
      rescue Exception
        # If we raise an exception here, EM dies, which causes problems (e.g.,
        # other Bud instances in the same process will crash). Ignoring the
        # error isn't best though -- we should do better (#74).
        puts "Exception handling network messages: #{$!}"
        puts "Inbound messages:"
        @bud.inbound.each do |m|
          puts "    #{m[1].inspect} (channel: #{m[0]})"
        end
        @bud.inbound.clear
      end

      @bud.rtracer.sleep if @bud.options[:rtrace]
    end

    def message_received(obj)
      unless (obj.class <= Array and obj.length == 2 and not
              @bud.tables[obj[0].to_sym].nil? and obj[1].class <= Array)
        raise BudError, "Bad inbound message of class #{obj.class}: #{obj.inspect}"
      end

      @bud.rtracer.recv(obj) if @bud.options[:rtrace]
      @bud.inbound << obj
    end
  end
end
