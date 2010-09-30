require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestBEDelivery < TestDelivery 
  def test_besteffort_delivery
    rd = spinup("BestEffortDelivery", 12345)
    sendtup = ['localhost:12346', 'localhost:12345', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)

    # transmission 'complete'
    rd.pipe_out.each do |p|
      print "P: #{p.inspect}\n"
    end

    assert_equal(1, rd.pipe_out.length)
    assert_equal(sendtup, rd.pipe_out.first)
  end
end
