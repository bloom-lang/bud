require './test_common'

class SimpleMax
  include Bud

  state do
    lmax :m
    lbool :done
  end

  bloom do
    done <= m.gt(12)
  end
end

class TestMax < MiniTest::Unit::TestCase
  def test_simple
    i = SimpleMax.new
    # assert_equal(2, i.strata.length)
    # strat_zero = i.stratum_collection_map[0]
    # [:m, :done].each {|r| assert(strat_zero.include? r) }
    i.m <+ Bud::MaxLattice.new(5)
    i.m <+ Bud::MaxLattice.new(10)
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.m <+ Bud::MaxLattice.new(13)
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def ntest_simple_implicit_merge
    i = SimpleMax.new
    # assert_equal(2, i.strata.length)
    # strat_zero = i.stratum_collection_map[0]
    # [:m, :done].each {|r| assert(strat_zero.include? r) }
    i.m <+ [5, 10]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.m <+ [13]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end
end
