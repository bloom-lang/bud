require 'rubygems'
require 'bud'
require 'lib/quorum_delivery'
require 'test/tc_delivery'


class TestBEDelivery < TestDelivery 
  def test_besteffort_delivery
    rd = spinup("BestEffortDelivery", 11115)
    sendtup = ['localhost:11116', 'localhost:11115', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    assert_equal(1, rd.pipe.length)
    assert_equal(sendtup, rd.pipe.first)

    soft_tick(rd)

    # transmission 'complete'
    assert_equal(1, rd.pipe_out.length)
    assert_equal(sendtup, rd.pipe_out.first)
  end
end
