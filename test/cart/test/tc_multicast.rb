require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'

require 'lib/multicast'

module TestState
  include Anise
  annotator :declare

  def state
    super
    table :mcast_done_perm, ['ident'], ['payload']
    table :rcv_perm, ['ident'], ['payload']
  end

  declare
  def mem
    mcast_done_perm <= mcast_done.map{|d| d } 
    rcv_perm <= pipe_chan.map{|r| [r.ident, r.payload] }
  end
end

class MC < Bud
  include TestState
  include BestEffortMulticast
end

class RMC < Bud
  include TestState
  include ReliableMulticast
end


class TestMC < TestLib
  def ntest_be
    mc = MC.new("localhost", 34256, {'dump' => true})
    mc2 = MC.new("localhost", 34257)
    mc3 = MC.new("localhost", 34258)

    assert_nothing_raised(RuntimeError) { mc.run_bg; mc2.run_bg; mc3.run_bg } 

    mc.members << ["localhost: 34257" ] 
    mc.members << ["localhost: 34258" ] 

    mc.send_mcast <+ [[1, 'foobar']] 

    advance(mc)
    advance(mc)
    assert_equal(1, mc.mcast_done_perm.length)
    assert_equal("foobar", mc.mcast_done_perm.first.payload)

    assert_equal(1, mc2.rcv_perm.length)
    assert_equal(1, mc3.rcv_perm.length)
    assert_equal("foobar", mc2.rcv_perm.first.payload)
  end

  def test_rd
    mc = RMC.new("localhost", 44256, {'dump' => true})
    mc2 = RMC.new("localhost", 44257, {})
    mc3 = RMC.new("localhost", 44258, {})

    assert_nothing_raised(RuntimeError) { mc.run_bg; mc2.run_bg; mc3.run_bg } 

    mc.members << ["localhost: 34257" ] 
    mc.members << ["localhost: 34258" ] 

    mc.send_mcast <+ [[1, 'foobar']] 


    advance(mc)
    advance(mc)
  end
  
end
