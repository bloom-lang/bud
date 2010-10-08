require 'rubygems'
require 'bud'
require 'test/test_lib'

require 'lib/lazy_cart'
require 'lib/imperative_cart_kvs'

class TestCart < TestLib
  def ntest_disorderly_cart
    program = BasicCartServer.new('localhost', 12345)
    program.run_bg
    sleep 1
    run_cart(program)
  end

  def test_destructive_cart
    program = ImperativeCartServer.new('localhost', 12345)
    program.run_bg
    run_cart(program)
  end

  def run_cart(program)

    add_members(program, "localhost:12345")

    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'A', 123])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 124])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'diapers', 'A', 125])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'meat', 'D', 126])

    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 127])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 128])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'A', 129])
    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'beer', 'D', 130])


    send_channel(program.ip, program.port, "checkout_msg", ['localhost:12345', 'localhost:12345',1234, 131])

    advance(program)
    advance(program)
    advance(program)
    advance(program)

    send_channel(program.ip, program.port, "action_msg", ['localhost:12345', 'localhost:12345', 1234, 'papers', 'A', 132])

    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    
    advance(program)    

    program.bigtable.each do |b|
      print "PBT: #{b.inspect}\n"
    end
    print "BTlen = #{program.bigtable.length}\n"
  
    assert_equal(2, program.memory.length)
    program.memory.each do |a|
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
    send_channel(program.ip, program.port, "checkout_msg", ['localhost:12345', 'localhost:12345',1234, 133])
    advance(program)
    advance(program)
    advance(program)
    advance(program)
    

    pcnt = 0
    program.memory.each do |a|
      print "MEMO: #{a.inspect}\n"
      pcnt = a.cnt if a.item == "papers"
    end

    print "pcnt is #{pcnt}\n"
  
    # undesirable but consistent that a 2nd checkout message should produce a revised manifest.
    assert_equal(3, program.memory.length)
    assert_equal(1, pcnt)
    
    
  end
end
