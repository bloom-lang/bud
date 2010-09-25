require 'rubygems'
require 'bud'
require 'cidr/reliable_delivery'
require 'test_lib'


class TestDelivery < TestLib
  def test_delivery
    t_delivery("BestEffortDelivery")
  end

  def test_reliable_delivery
    t_delivery("ReliableDelivery")
    f_delivery("ReliableDelivery")
  end

  def t_delivery(type)
    rd = spinup(type, 12345)
    rd2 = spinup(type, 12346)
    assert_nothing_raised(RuntimeError){rd2.run_bg}
    rd.pipe <+ [['localhost:12346', 'localhost:12345', 1, 'foobar']]
    assert_nothing_raised(RuntimeError){rd.run_bg}
    sleep 1 
    # received at destination
    rd2.pipe_chan.each do |pc|
      assert_equal(1, pc[2])
    end
    # ack'd at sender
    rd.pipe_out.each do |p|
      print "pipe_out data, etc: #{p.inspect}\n"
      assert_equal(1, p[2])
    end
    spinout(rd)
    spinout(rd2)
  end

  def f_delivery(type)
    rd = spinup(type, 12345)
    rd.pipe <+ [['localhost:12346', 'localhost:12345', 1, 'foobar']]
    assert_nothing_raised(RuntimeError){rd.run_bg}

    # NOT ack'd at sender if there was no receiver!
    assert_equal(0, rd.pipe_out.length)
   end 
      
  

  def spinup(type, port)
    d = eval "#{type}.new('localhost', 12345)" 
    d.tick
    return d
  end

  def spinout(b)
    (0..2).each do |i|
      assert_nothing_raised(RuntimeError){advance(b)}
    end
  end

end
