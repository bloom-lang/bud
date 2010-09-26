require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestReliableDelivery < TestDelivery 
  def test_reliable_delivery
    # reliable does basic
    master = t_delivery("ReliableDelivery")


    print "OK\n"
    assert_equal(1, master.pipe_out.length)
    master.pipe_out.each do |p|
      assert_equal(1, p[2])
    end

    #f_delivery("ReliableDelivery")

  end

end
