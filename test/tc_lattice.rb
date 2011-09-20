require 'test_common'

class SimpleMax
  include Bud

  def initialize(use_scratch=false)
    @use_scratch = use_scratch
    # NB: Calling super() after we set @use_scratch is important, since
    # Bud#initialize invokes the state methods.
    super()
  end

  state do
    lat_max :m, :scratch => @use_scratch
    scratch :inputt, [:val]
    scratch :done, [:t]
  end

  bloom do
    m <= inputt
    done <= m.gt_k(10) { [[true]] }
  end
end

class SimpleMaxReveal < SimpleMax
  state do
    scratch :current_val, [:v]
  end

  bloom do
    current_val <= m.reveal
  end
end

class MaxOfMax
  include Bud

  state do
    lat_max :m1
    lat_max :m2
    lat_max :m3
    scratch :inputt, [:val]
    # XXX: make this lat_bool?
    scratch :done, [:t]
  end

  bloom do
    m1 <= inputt {|t| t if t.val % 2 == 0}
    m2 <= inputt {|t| t if t.val % 2 == 1}
    m3 <= m1
    m3 <= m2
    done <= m3.gt_k(10) { [[true]] }
  end
end

class ComposeLattice
  include Bud

  state do
    lat_max :m1
    lat_max :m2
    lat_vec :m3
    scratch :inputt, [:val]
    # XXX: make this lat_bool?
    scratch :done, [:t]
  end

  bloom do
    m1 <= inputt {|t| t if t.val % 2 == 0}
    m1 <= inputt {|t| t if t.val % 2 == 1}
    m3 <= (m1 * m2)
    done <= m3.all?(:gt_k, 10)
  end
end

class TestMaxLattice < Test::Unit::TestCase
  def test_simple_max
    i = SimpleMax.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m, :done].each {|r| assert(strat_zero.include? r) }
    i.inputt <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [[12]]
    i.tick
    assert_equal([[true]], i.done.to_a)
    i.tick
    assert_equal([[true]], i.done.to_a)
  end

  def test_max_reveal_nm
    i = SimpleMaxReveal.new
    assert_equal(3, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m, :done].each {|r| assert(strat_zero.include? r) }
    assert_equal(false, strat_zero.include?(:current_val))
    strat_one = i.stratum_collection_map[1]
    assert(strat_one.include? :current_val)
    i.inputt <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
    assert_equal([[3]], i.current_val.to_a)
    i.inputt <+ [[12]]
    i.tick
    assert_equal(false, i.done.empty?)
    assert_equal([[12]], i.current_val.to_a)
  end

  def test_max_scratch
    i = SimpleMax.new(true)
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m, :done].each {|r| assert(strat_zero.include? r) }
    i.inputt <+ [[8], [12]]
    i.tick
    assert_equal([[true]], i.done.to_a)
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [[6]]
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [[1], [14]]
    i.tick
    assert_equal([[true]], i.done.to_a)
  end

  def test_max_of_max
    i = MaxOfMax.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m1, :m2, :m3, :done].each {|r| assert(strat_zero.include? r) }
    i.inputt <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [[12]]
    i.tick
    assert_equal(false, i.done.empty?)
  end

  def test_compose
    i = ComposeLattice.new
    # XXX: check stratification
    i.inputt <+ [[4], [12]]
    i.tick
    assert(i.done.empty?)
  end
end

class SimpleBool
  include Bud

  state do
    lat_bool :b
    scratch :inputt, [:val]
    scratch :done, [:t]
  end

  bloom do
    b <= inputt {|t| true if t[0] == "remedy"}
    done <= b.when_true { [["yay"]] }
  end
end

class TestBoolLattice < Test::Unit::TestCase
  def test_bool
    i = SimpleBool.new
    assert(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:b, :done].each {|r| assert(strat_zero.include? r)}
    i.inputt <+ [["bica"], ["cole"]]
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [["remedy"]]
    i.tick
    assert_equal(false, i.done.empty?)
    i.inputt <+ [["philz"]]
    i.tick
    assert_equal(false, i.done.empty?)
  end
end
