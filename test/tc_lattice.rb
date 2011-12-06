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

class MaxOverChannel
  include Bud

  state do
    scratch :do_send, [:addr]
    scratch :in_t, [:v]
    channel :chn, [:@addr] => [:v]
    table :chn_log, [] => [:v]
    lmax :m
  end

  bloom do
    chn <~ do_send {|t| [t.addr, m]}
    chn_log <= chn {|c| [c.v]}
    m <= in_t {|t| t[0]}
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

  def test_max_over_chn
    src, dst = Array.new(2) { MaxOverChannel.new }
    [src, dst].each {|n| n.run_bg }

    expected_val = nil
    q = Queue.new
    dst.register_callback(:chn) do |t|
      assert_equal(1, t.length)
      assert_equal(expected_val, t.first.v.reveal)
      q.push(true)
    end

    src.sync_do {
      src.do_send <+ [[dst.ip_port]]
    }
    q.pop
    dst.sync_do {
      assert_equal(1, dst.chn_log.length)
      assert_equal(nil, dst.chn_log.first.v.reveal)
    }

    expected_val = 30
    src.sync_do {
      src.m <+ [2, 15, 0, 10, 7, 20]
      src.in_t <+ [[16], [30]]
      src.do_send <+ [[dst.ip_port]]
    }
    q.pop
    dst.sync_do {
      assert_equal(1, dst.chn_log.length)
      assert_equal(30, dst.chn_log.first.v.reveal)
    }

    [src, dst].each {|n| n.stop }
  end
end

# Based on Example 3.1 in "Monotonic Aggregation in Deductive Databases" (Ross
# and Sagiv, PODS'92).
class ShortestPaths
  include Bud

  state do
    table :arc, [:from, :to, :c]
    table :path, [:from, :to, :next] => [:c]
    table :min_cost, [:from, :to] => [:c]
  end

  bloom do
    min_cost <= path {|p| [p.from, p.to, p.c]}
    path <= arc {|a| [a.from, a.to, "direct", Bud::MinLattice.new(a.c)]}
    path <= (min_cost * arc).pairs(:to => :from) do |m,a|
      [m.from, a.to, a.from, m.c + a.c]
    end
  end
end

class TestShortestPaths < Test::Unit::TestCase
  def test_simple
    i = ShortestPaths.new
    i.arc <+ [["a", "b", 11],
              ["a", "b", 10],
              ["a", "c", 15],
              ["b", "c", 20],
              ["b", "d", 30],
              ["c", "d", 5],
              ["d", "e", 10]]
    i.tick
    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next, t.c.reveal]}
    assert_equal([["a", "b", "direct", 10],
                  ["a", "c", "b", 30],
                  ["a", "c", "direct", 15],
                  ["a", "d", "b", 40],
                  ["a", "d", "c", 20],
                  ["a", "e", "d", 30],
                  ["b", "c", "direct", 20],
                  ["b", "d", "c", 25],
                  ["b", "d", "direct", 30],
                  ["b", "e", "d", 35],
                  ["c", "d", "direct", 5],
                  ["c", "e", "d", 15],
                  ["d", "e", "direct", 10]], path_r.sort)

    min_cost_r = i.min_cost.to_a.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["a", "b", 10],
                  ["a", "c", 15],
                  ["a", "d", 20],
                  ["a", "e", 30],
                  ["b", "c", 20],
                  ["b", "d", 25],
                  ["b", "e", 35],
                  ["c", "d", 5],
                  ["c", "e", 15],
                  ["d", "e", 10]], min_cost_r.sort)
  end

  def test_cyclic
    i = ShortestPaths.new
    i.arc <+ [["a", "b", 20],
              ["b", "a", 5],
              ["b", "c", 10],
              ["a", "c", 35],
              ["d", "a", 15],
              ["d", "b", 5]]
    i.tick

    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next, t.c.reveal]}
    assert_equal([["a", "a", "b", 25],
                  ["a", "b", "a", 45],
                  ["a", "b", "direct", 20],
                  ["a", "c", "a", 60],
                  ["a", "c", "b", 30],
                  ["a", "c", "direct", 35],
                  ["b", "a", "b", 30],
                  ["b", "a", "direct", 5],
                  ["b", "b", "a", 25],
                  ["b", "c", "a", 40],
                  ["b", "c", "b", 35],
                  ["b", "c", "direct", 10],
                  ["d", "a", "b", 10],
                  ["d", "a", "direct", 15],
                  ["d", "b", "a", 30],
                  ["d", "b", "direct", 5],
                  ["d", "c", "a", 45],
                  ["d", "c", "b", 15]], path_r.sort)

    min_cost_r = i.min_cost.to_a.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["a", "a", 25],
                  ["a", "b", 20],
                  ["a", "c", 30],
                  ["b", "a", 5],
                  ["b", "b", 25],
                  ["b", "c", 10],
                  ["d", "a", 10],
                  ["d", "b", 5],
                  ["d", "c", 15]], min_cost_r.sort)
  end
end
