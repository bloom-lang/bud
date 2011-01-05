require 'rubygems'
require 'bud'
require 'lib/quorum_delivery'
require 'test/delivery_lib'

class TestBEDelivery < TestDelivery 
  def test_delivery1
    rd = spinup("RED", 12222)
    sendtup = ['localhost:12223', 'localhost:12222', 1, 'foobar']
    rd.pipe_in <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd)

    # transmission not 'complete'
    assert_equal(0, rd.pipe_perm.length)
  end


  def test_besteffort_delivery2
    rd = spinup("RED", 13333)
    rd2 = spinup("RED", 13334)
    sendtup = ['localhost:13334', 'localhost:13333', 1, 'foobar']
    rd.pipe_in <+ [ sendtup ]
    soft_tick(rd)
    soft_tick(rd2)
    soft_tick(rd)
    sleep 1

    # debugging
    assert_equal(1, rd.pipe.length)

    # transmission 'complete'
    assert_equal(1, rd.teststuff_pipe_perm.length)
    #assert_equal(1, rd.pipe_perm.length)
  end


end
