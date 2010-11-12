require 'rubygems'
require 'bud'
require 'test/test_lib'
require 'test/cart_workloads'

require 'lib/disorderly_cart'
#require 'lib/imperative_cart_kvs'

class TestCart < TestLib
  include CartWorkloads

  def test_disorderly_cart
    program = BasicCartServer.new('localhost', 12345)
    program.run_bg
    sleep 1
    run_cart(program)
    advance(program)

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
