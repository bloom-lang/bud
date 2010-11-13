require 'rubygems'
require 'bud'
require 'lib/quorum_delivery'
require 'test/delivery_lib'

class TestBEDelivery < TestDelivery 
  def test_besteffort_delivery
    #rd = spinup("BestEffortDelivery", 11115)
    rd = spinup("BED", 11115)
    sendtup = ['localhost:11116', 'localhost:11115', 1, 'foobar']
    rd.pipe_in <+ [ sendtup ]
    soft_tick(rd)
    assert_equal(1, rd.pipe_perm.length)
    assert_equal(sendtup, rd.pipe_perm.first)

    soft_tick(rd)

    # transmission 'complete'
    assert_equal(1, rd.pipe_perm.length)
    assert_equal(sendtup, rd.pipe_perm.first)
  end
end
