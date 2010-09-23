require 'rubygems'
require 'bud'
require 'test/unit'
require 'lib/cart'

class TestCart < Test::Unit::TestCase
  def advance(p)
    sleep 1
    send_channel(p, "tickler", ["#{p.ip}:#{p.port}"])
  end

  def send_channel(c, chan, payload)
    EventMachine::connect(c.ip, c.port) do |c|
      pl = ([chan, payload]).to_msgpack
      c.send_data(pl)
    end
    
  end
  
  def test_cart
    program = BasicCartServer.new('localhost', 12345)

    program.tick

    program.run_bg
    sleep 1

    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'A', 123])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 124])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'diapers', 'A', 125])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'D', 126])

    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 127])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 128])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 129])
    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'D', 130])
  

    send_channel(program, "checkout", ['localhost:12345', 'localhost:12345',1234])

    advance(program)
    advance(program)
    advance(program)
    advance(program)

    send_channel(program, "action", ['localhost:12345', 'localhost:12345', 1234, 'papers', 'A', 131])

    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    

    assert_equal(2, program.status.length)
    program.status.each do |a|
      print "item: #{a.inspect}\n"
      if a.item == "beer"
        assert_equal(3, a.cnt)
      elsif a.item == "diapers"
        assert_equal(1, a.cnt)
      else
        assert_error("incorrect item #{a.item} in cart")
      end
    end

    # the checkout message is redelivered!
    send_channel(program, "checkout", ['localhost:12345', 'localhost:12345',1234])
    advance(program)
    

    pcnt = 0
    program.status.each do |a|
      pcnt = a.cnt if a.item == "papers"
    end
  
    # undesirable but consistent that a 2nd checkout message should produce a revised manifest.
    assert_equal(3, program.status.length)
    assert_equal(1, pcnt)
    
    
  end
end
