require 'rubygems'
require 'bud'
require 'cidr/reliable_delivery'
require 'test/unit'
require 'test_lib'


class TestRD < TestLib

  def test_rd
    rd = ReliableDelivery.new('localhost', 12345)
    rd.tick

    rd2 = ReliableDelivery.new('localhost', 12346)
    rd2.tick
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
      assert_equal(1, p[2])
    end
    
    (0..2).each do |i|
      assert_nothing_raised(RuntimeError){advance(rd)}
      assert_nothing_raised(RuntimeError){advance(rd2)}
    end



  end
end
