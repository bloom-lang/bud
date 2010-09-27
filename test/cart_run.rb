require 'rubygems'
require 'bud'
require 'cidr/cart'
require 'cidr/lazy_cart'

##require 'lib/imperative_cart'
require 'cidr/imperative_cart_kvs'

  @@me = 'ip-10-203-91-184.ec2.internal'
  @@myport = 12345
  @@meadd = "#{@@me}:#{@myport}"
  @@peer1 = 'ip-10-202-70-4.ec2.internal'
  @@peer1port = 12346
  @@peer1add = "#{@@peer1}:#{@@peer1port}"



class CartStuff
  #@@me = 'ip-10-203-91-184.ec2.internal'
  #@@myport = 12345
  #@@meadd = "#{@@me}:#{@myport}"
  #@@peer1 = 'ip-10-202-70-4.ec2.internal'
  ##@@peer1port = 12346
  #@@peer1add = "#{@@peer1}:#{@@peer1port}"


  attr_reader :bud
  def initialize(ip, port)
    if ARGV[1] == "imp"
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
    print "try to tick\n"
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
    @bud.member << [@@meadd]
    @bud.member << [@@peer1add]
    @bud.member << ['localhost:12347']
  end
  def shop

  end
end


if ARGV[0] == 'm'
	c = CartStuff.new(@@me, @@myport)
else
	c = CartStuff.new(@@peer1, @@peer1port)
end
c.run
sleep 2

if ARGV[2] == "imp"
  chan = "iaction"
else
  chan = "action"
end



    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'meat', 'A', 123])

    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'beer', 'A', 124])
    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'diapers', 'A', 125])
    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'meat', 'D', 126])

    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'beer', 'A', 127])
    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'beer', 'A', 128])
    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'beer', 'A', 129])
    c.send_channel(chan, [@@meadd, @@meadd, 1234, 'beer', 'D', 130])


    c.send_channel("checkout", [@@meadd, @@meadd,1234])


    #(0..3).each do |i|
    #  print "ADV #{i}\n"
    #  c.advance
      #c.advancer("localhost", 12346)
    #end

   #c.bud.status.each {|s| print "STATUS: #{s.inspect}\n" }
