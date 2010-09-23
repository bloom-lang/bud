require 'rubygems'
require 'bud'
require 'lib/cart'

require 'lib/imperative_cart'

class CartStuff
  attr_reader :bud
  def initialize(ip, port)
    if ARGV[2] == "imp"
      @bud = ImperativeCartServer.new(ip, port)
    else
      @bud = BasicCartServer.new(ip, port)
    end
    @bud.tick
    add_members
  end

  def run
    if @bud.port == 12345
      @bud.run_bg
      print "OUT\n"
    else
      @bud.run
    end
  end

  def advancer(ip, port)
    sleep 1
    send_channel("tickler", ["#{ip}:#{port}"])
    print "Advanced\n"
  end

  def advance
    advancer(@bud.ip, @bud.port)
  end

  def send_channel(chan, payload)
    EventMachine::connect(@bud.ip, @bud.port) do |c|
      pl = ([chan, payload]).to_msgpack
      c.send_data(pl)
    end
  end

  def add_members
    @bud.tick
    @bud.member << ['localhost:12345']
    @bud.member << ['localhost:12346']
    @bud.member << ['localhost:12347']
  end
end



c = CartStuff.new(ARGV[0], ARGV[1])
c.run
sleep 2

    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'A', 123])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 124])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'diapers', 'A', 125])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'D', 126])

    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 127])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 128])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 129])
    c.send_channel("action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'D', 130])


    c.send_channel("checkout", ['localhost:12345', 'localhost:12345',1234])


    (0..10).each do |i|
      print "ADV #{i}\n"
      c.advance
      c.advancer("localhost", 12346)
    end

   c.bud.status.each {|s| print "STATUS: #{s.inspect}\n" }
