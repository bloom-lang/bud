require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestBEDelivery < TestDelivery 
  def test_besteffort_delivery1
    rd = spinup("ReliableDelivery", 12345)
    sendtup = ['localhost:12346', 'localhost:12345', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)

    # transmission not 'complete'
    assert_equal(0, rd.pipe_out.length)
  end


  def test_besteffort_delivery2
    rd = spinup("ReliableDelivery", 12346)
    rd2 = spinup("ReliableDelivery", 12347)
    sendtup = ['localhost:12347', 'localhost:12346', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)
    sleep 1

    # transmission 'complete'
    assert_equal(1, rd.pipe_out.length)
  end


end
