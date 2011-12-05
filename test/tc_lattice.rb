require 'test_common'

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

class MaxOfMax
  include Bud

  state do
    scratch :in_t, [:v]
    lmax :m1
    lmax :m2
    lmax :m3
    lbool :done
  end

  bloom do
    m1 <= in_t {|t| t[0] if t[0] % 2 == 0}
    m2 <= in_t {|t| t[0] if t[0] % 2 == 1}
    m3 <= m1
    m3 <= m2
    done <= m3.gt(20)
  end
end

class EmbedMax
  include Bud

  state do
    table :t
    scratch :in_t, [:v]
    lmax :m1
    lmax :m2
  end

  bloom do
    t <= [["m1", m1]]
    t <= [["m2", m2]]

    m1 <= in_t {|t| t[0] if t[0] % 2 == 0}
    m2 <= in_t {|t| t[0] if t[0] % 2 == 1}
  end
end

class TestMax < Test::Unit::TestCase
  def test_simple
    i = SimpleMax.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m, :done].each {|r| assert(strat_zero.include? r) }
    i.m <+ [5, 10]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.m <+ [15]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_max_of_max
    i = MaxOfMax.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:in_t, :m1, :m2, :m3, :done].each {|r| assert(strat_zero.include? r) }
    i.in_t <+ [[4], [6], [7]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[22], [19]]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
    i.in_t <+ [[2], [3], [23]]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_embed_max
    i = EmbedMax.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:in_t, :t, :m1, :m2].each {|r| assert(strat_zero.include? r) }
    i.tick
    i.in_t <+ [[5], [10], [7], [2], [13]]
    i.tick
    assert_equal(10, i.t[["m1"]].val.reveal)
    assert_equal(13, i.t[["m2"]].val.reveal)
    i.in_t <+ [[1], [2]]
    i.tick
    assert_equal(10, i.t[["m1"]].val.reveal)
    assert_equal(13, i.t[["m2"]].val.reveal)
    i.in_t <+ [[15], [16], [17]]
    i.tick
    assert_equal(16, i.t[["m1"]].val.reveal)
    assert_equal(17, i.t[["m2"]].val.reveal)
  end
end
