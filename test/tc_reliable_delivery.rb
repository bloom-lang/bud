require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestBEDelivery < TestDelivery 
  def test_besteffort_delivery1
    rd = spinup("ReliableDelivery", 12222)
    sendtup = ['localhost:12223', 'localhost:12222', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)

    # transmission not 'complete'
    assert_equal(0, rd.pipe_out.length)
  end


  def test_besteffort_delivery2
    rd = spinup("ReliableDelivery", 13333)
    rd2 = spinup("ReliableDelivery", 13334)
    sendtup = ['localhost:13334', 'localhost:13333', 1, 'foobar']
    rd.pipe <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)
    sleep 1

    # transmission 'complete'
    assert_equal(1, rd.pipe_out.length)
  end


end
