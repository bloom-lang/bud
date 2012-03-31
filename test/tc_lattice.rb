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

class EmptyMaxMerge
  include Bud

  state do
    lmax :m1
    lmax :m2
  end

  bootstrap do
    m1 <= Bud::MaxLattice.new(5)
  end

  bloom do
    m1 <= m2
  end
end

class TestMax < MiniTest::Unit::TestCase
  def test_simple
    i = SimpleMax.new
    %w[m done].each {|r| assert_equal(0, i.collection_stratum(r))}
    i.m <+ Bud::MaxLattice.new(5)
    i.m <+ Bud::MaxLattice.new(10)
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.m <+ Bud::MaxLattice.new(13)
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_simple_implicit_merge
    i = SimpleMax.new
    %w[m done].each {|r| assert_equal(0, i.collection_stratum(r))}
    i.m <+ [5, 10]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.m <+ [13]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_max_of_max
    i = MaxOfMax.new
    %w[in_t m1 m2 m3 done].each {|r| assert_equal(0, i.collection_stratum(r))}
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

  def test_empty_max
    i = EmptyMaxMerge.new
    i.tick
  end
end

class MapIntersect
  include Bud

  state do
    lmap :m1
    lmap :m2
    lmap :m3
    lmap :m4
    lbool :done_m3
    lbool :done_m4
  end

  bloom do
    m3 <= m1.intersect(m2)
    m4 <= m2.intersect(m1)

    done_m3 <= m3.size.gt_eq(2)
    done_m4 <= m4.size.gt_eq(2)
  end
end

class TestMap < MiniTest::Unit::TestCase
  def ntest_map_intersect
    i = MapIntersect.new
    %w[m1 m2 m3 m4 done_m3 done_m4].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end
    i.tick
    assert_equal([], get_val_for_map(i, :m3))
    assert_equal([], get_val_for_map(i, :m4))

    i.m1 <+ [{"x" => Bud::MaxLattice.new(15)}]
    i.m2 <+ [{"y" => Bud::MaxLattice.new(20)}]
    i.tick
    assert_equal([], get_val_for_map(i, :m3))
    assert_equal([], get_val_for_map(i, :m4))
    assert_equal(false, i.done_m3.current_value.reveal)
    assert_equal(false, i.done_m4.current_value.reveal)

    i.m1 <+ [{"y" => Bud::MaxLattice.new(25)}]
    i.m2 <+ [{"z" => Bud::MaxLattice.new(30)}]
    i.tick
    assert_equal([["y", 25]], get_val_for_map(i, :m3))
    assert_equal([["y", 25]], get_val_for_map(i, :m4))
    assert_equal(false, i.done_m3.current_value.reveal)
    assert_equal(false, i.done_m4.current_value.reveal)

    i.m1 <+ [{"y" => Bud::MaxLattice.new(31)}, {"z" => Bud::MaxLattice.new(32)}]
    i.tick
    assert_equal([["y", 31], ["z", 32]], get_val_for_map(i, :m3))
    assert_equal([["y", 31], ["z", 32]], get_val_for_map(i, :m4))
    assert_equal(true, i.done_m3.current_value.reveal)
    assert_equal(true, i.done_m4.current_value.reveal)
  end
end
