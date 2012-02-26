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

class MaxErrors
  include Bud

  state do
    table :t
    lmax :m
  end

  bloom do
    m <= t {|t| t.val}
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

  def test_key_error
    i = MaxErrors.new
    assert_raise(Bud::TypeError) do
      i.t <+ [[Bud::MaxLattice.new(5), "v"]]
    end
  end

  def test_merge_type_error
    i = MaxErrors.new
    i.t <+ [["y", self.class]]
    assert_raise(Bud::TypeError) do
      i.tick
    end
  end

  def test_empty_max
    i = EmptyMaxMerge.new
    assert_nothing_raised { i.tick }
  end
end

# Based on Example 3.1 in "Monotonic Aggregation in Deductive Databases" (Ross
# and Sagiv, PODS'92). Unlike in R&S, we don't need to compute min_cost as a
# separate relation, although we do so for testing purposes.
class ShortestPathsL
  include Bud

  state do
    table :link, [:from, :to, :c]
    table :path, [:from, :to, :next] => [:c]
    table :min_cost, [:from, :to] => [:c]
  end

  bloom do
    path <= link {|l| [l.from, l.to, "direct", Bud::MinLattice.new(l.c)]}
    path <= (link * path).pairs(:to => :from) do |l,p|
      [l.from, p.to, l.to, p.c + l.c]
    end
    min_cost <= path {|p| [p.from, p.to, p.c]}
  end
end

# Compute shortest paths in a slightly different manner (closer to R&S): add
# links to the end of a previously-discovered path, rather than prepending them
# to the beginning; this means the "next" field actually points backward from
# the end of the path.
class ShortestPathsVariant
  include Bud

  state do
    table :link, [:from, :to, :c]
    table :path, [:from, :to, :next] => [:c]
    table :min_cost, [:from, :to] => [:c]
  end

  bloom do
    path <= link {|l| [l.from, l.to, "direct", Bud::MinLattice.new(l.c)]}
    path <= (path * link).pairs(:to => :from) do |p,l|
      [p.from, l.to, l.from, p.c + l.c]
    end
    min_cost <= path {|p| [p.from, p.to, p.c]}
  end
end

# Find the maximum capacity path ("widest path") between each pair of nodes;
# that is, the path that maximizes the cost of the minimum-cost link in the path.
# Note that while the shortest path programs are actually defined over
# multigraphs, we require only a single edge between nodes (mostly for
# convenience).
class MaxCapacityPaths
  include Bud

  state do
    table :link, [:from, :to] => [:c]
    table :path, [:from, :to, :next] => [:c]
    table :max_cap, [:from, :to] => [:c]
  end

  bloom do
    path <= link {|l| [l.from, l.to, "direct", Bud::MaxLattice.new(l.c)]}
    path <= (link * path).pairs(:to => :from) do |l,p|
      [l.from, p.to, l.to, p.c.min_of(l.c)]
    end
    max_cap <= path {|p| [p.from, p.to, p.c]}
  end
end

# Compute all paths (transitive closure). This is done entirely using set
# lattices, rather than via a combination of lattices and set-oriented
# collections.
class AllPathsL
  include Bud

  state do
    lset :link
    lset :path
  end

  bootstrap do
    link <= [[['a', 'b', 1], ['a', 'b', 4],
              ['b', 'c', 1], ['c', 'd', 1],
              ['d', 'e', 1]]]
  end

  bloom do
    path <= link
    path <= path.product(link).pro do |p,l|
      [[p[0], l[1], p[2] + l[2]]] if p[1] == l[0]
    end
  end
end

class TestGraphPrograms < Test::Unit::TestCase
  def test_spath_simple
    i = ShortestPathsL.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:link, :path, :min_cost].each {|r| assert(strat_zero.include? r) }
    i.link <+ [["a", "b", 11],
               ["a", "b", 10],
               ["a", "c", 15],
               ["b", "c", 20],
               ["b", "c", 21],
               ["b", "d", 30],
               ["c", "d", 5],
               ["d", "e", 10]]
    i.tick
    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next, t.c.reveal]}
    assert_equal([["a", "b", "direct", 10],
                  ["a", "c", "b", 30],
                  ["a", "c", "direct", 15],
                  ["a", "d", "b", 35],
                  ["a", "d", "c", 20],
                  ["a", "e", "b", 45],
                  ["a", "e", "c", 30],
                  ["b", "c", "direct", 20],
                  ["b", "d", "c", 25],
                  ["b", "d", "direct", 30],
                  ["b", "e", "c", 35],
                  ["b", "e", "d", 40],
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

  def test_spath_cyclic
    i = ShortestPathsL.new
    i.link <+ [["a", "b", 20],
               ["a", "b", 21],
               ["b", "a", 5],
               ["b", "a", 8],
               ["b", "c", 10],
               ["b", "c", 12],
               ["a", "c", 35],
               ["d", "a", 15],
               ["d", "b", 5]]
    i.tick

    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next, t.c.reveal]}
    assert_equal([["a", "a", "b", 25],
                  ["a", "b", "b", 45],
                  ["a", "b", "direct", 20],
                  ["a", "c", "b", 30],
                  ["a", "c", "direct", 35],
                  ["b", "a", "a", 30],
                  ["b", "a", "direct", 5],
                  ["b", "b", "a", 25],
                  ["b", "c", "a", 35],
                  ["b", "c", "direct", 10],
                  ["d", "a", "a", 40],
                  ["d", "a", "b", 10],
                  ["d", "a", "direct", 15],
                  ["d", "b", "a", 35],
                  ["d", "b", "b", 30],
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

  def test_spath_cyclic_variant
    i = ShortestPathsVariant.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:link, :path, :min_cost].each {|r| assert(strat_zero.include? r) }
    i.link <+ [["a", "b", 20],
               ["a", "b", 24],
               ["b", "a", 5],
               ["b", "a", 8],
               ["b", "c", 10],
               ["b", "c", 11],
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

  def test_maxcap_simple
    i = MaxCapacityPaths.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:link, :path, :max_cap].each {|r| assert(strat_zero.include? r) }

    i.link <+ [["a", "b", 5], ["b", "c", 7]]
    i.tick
    res = i.max_cap.to_a.sort.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["a", "b", 5], ["a", "c", 5], ["b", "c", 7]], res)

    i.link <+ [["a", "d", 8], ["d", "b", 9]]
    i.tick
    res = i.max_cap.to_a.sort.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["a", "b", 8],
                  ["a", "c", 7],
                  ["a", "d", 8],
                  ["b", "c", 7],
                  ["d", "b", 9],
                  ["d", "c", 7]], res)

    i.link <+ [["a", "e", 1], ["e", "b", 2]]
    i.tick
    res = i.max_cap.to_a.sort.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["a", "b", 8],
                  ["a", "c", 7],
                  ["a", "d", 8],
                  ["a", "e", 1],
                  ["b", "c", 7],
                  ["d", "b", 9],
                  ["d", "c", 7],
                  ["e", "b", 2],
                  ["e", "c", 2]], res)
  end

  def test_all_paths
    i = AllPathsL.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:link, :path].each {|r| assert(strat_zero.include? r) }

    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["b", "c", 1], ["b", "d", 2], ["b", "e", 3], ["c", "d", 1],
                  ["c", "e", 2], ["d", "e", 1]], i.path.current_value.reveal.sort)

    i.link <+ [[['e', 'f', 1]]]
    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["a", "f", 5], ["a", "f", 8], ["b", "c", 1], ["b", "d", 2],
                  ["b", "e", 3], ["b", "f", 4], ["c", "d", 1], ["c", "e", 2],
                  ["c", "f", 3], ["d", "e", 1], ["d", "f", 2], ["e", "f", 1]],
                 i.path.current_value.reveal.sort)
  end
end

class SimpleMap
  include Bud

  state do
    lmap :h
    lmax :m1
    lmax :m2
    lset :hkeys
    scratch :in_t, [:v]
  end

  bloom do
    h <= [{"x" => m1, "y" => m1}]
    h <= in_t {|t| {t.v => m2}}
    hkeys <= h.key_set
  end
end

class MapWithPro < SimpleMap
  state do
    scratch :out_t
  end

  bloom do
    out_t <= h {|k,v| v.gt(10).when_true {
        [k, v + 1] if k != "x"
      }
    }
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

class TestMap < Test::Unit::TestCase
  def get_val_for_map(i, r)
    i.send(r).current_value.reveal.map {|k,v| [k, v.reveal]}.sort
  end

  def test_map_simple
    i = SimpleMap.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:h, :m1, :m2, :in_t].each {|r| assert(strat_zero.include? r) }

    i.m1 <+ [5, 12, 3]
    i.m2 <+ [3, 4, 5]
    i.in_t <+ [["y"], ["z"]]
    i.tick
    assert_equal([["x", 12], ["y", 12], ["z", 5]], get_val_for_map(i, :h))
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)

    i.m2 <+ [15]
    i.tick
    assert_equal([["x", 12], ["y", 12], ["z", 5]], get_val_for_map(i, :h))
    assert_equal(15, i.m2.current_value.reveal)
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)

    i.m2 <+ [13]
    i.in_t <+ [["y"], ["z"]]
    i.tick
    assert_equal([["x", 12], ["y", 15], ["z", 15]], get_val_for_map(i, :h))
    assert_equal(15, i.m2.current_value.reveal)
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)
  end

  def test_map_pro
    i = MapWithPro.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:h, :m1, :m2, :in_t, :out_t].each {|r| assert(strat_zero.include? r) }

    i.m1 <+ [1, 2, 12]
    i.m2 <+ [3, 4, 5]
    i.in_t <+ [["z"]]
    i.tick

    out_val = i.out_t.to_a.map {|k,v| [k, v.reveal]}
    assert_equal([["y", 13]], out_val.sort)
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)
  end

  def test_map_intersect
    i = MapIntersect.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:m1, :m2, :m3, :m4, :done_m3, :done_m4].each {|r| assert(strat_zero.include? r) }
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

class SimpleSet
  include Bud

  state do
    lset :s1
    lset :s2
    lset :s3
    lbool :done
    scratch :in_t, [:v]
  end

  bloom do
    s1 <= in_t {|t| [t.v]}
    s3 <= s1.intersect(s2)
    done <= s3.size.gt(3)
  end
end

class SetWithPro < SimpleSet
  state do
    scratch :out_t, [:v]
  end

  bloom do
    out_t <= s3 {|s| [s + 10] unless s == 3}
  end
end

class SetProduct
  include Bud

  state do
    lset :s1
    lset :s2
    lset :s3
  end

  bloom do
    s3 <= s1.product(s2)
  end
end

class TestSet < Test::Unit::TestCase
  def test_set_simple
    i = SimpleSet.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:s1, :done, :in_t].each do |r|
      assert(strat_zero.include? r)
    end

    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[2], [3]]
    i.s2 <+ [[5], [6], [7]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[3], [5], [6]]
    i.s2 <+ [[12]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[12]]
    i.s2 <+ [[2], [14]]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_set_pro
    i = SetWithPro.new
    i.tick
    assert_equal([], i.out_t.to_a.sort)

    i.in_t <+ [[5], [6]]
    i.s2 <+ [[3], [6]]
    i.tick
    assert_equal([[16]], i.out_t.to_a.sort)
    i.in_t <+ [[3], [7]]
    i.s2 <+ [[8]]
    i.tick
    assert_equal([[16]], i.out_t.to_a.sort)
    i.s2 <+ [[7]]
    i.tick
    assert_equal([[16], [17]], i.out_t.to_a.sort)
  end

  def test_set_product
    i = SetProduct.new
    i.tick
    assert_equal([], i.s3.current_value.reveal)

    i.s1 <+ [[1], [2]]
    i.tick
    assert_equal([], i.s3.current_value.reveal)

    i.s2 <+ [[3]]
    i.tick
    assert_equal([[1,3], [2,3]], i.s3.current_value.reveal.sort)

    i.s1 <+ [[3]]
    i.s2 <+ [[7]]
    i.tick
    assert_equal([[1,3], [1,7], [2,3], [2,7], [3,3], [3,7]],
                 i.s3.current_value.reveal.sort)
  end

  # We want to check that the set lattice eliminates duplicates from its input,
  # not just after application of the merge function. Since merges are called
  # repeatedly during actual query execution, we need to test the set lattice's
  # behavior outside the runtime.
  def test_set_dup_elim
    s = Bud::SetLattice.new([1,1,1,2,2])
    assert_equal(false, s.size.gt(2).reveal)

    s = s.merge(Bud::SetLattice.new([1,2]))
    assert_equal(false, s.size.gt(2).reveal)

    s = s.merge(Bud::SetLattice.new([3]))
    assert_equal(true, s.size.gt(2).reveal)
  end
end

class SimpleSum
  include Bud

  state do
    scratch :in_t, [:which, :v]
    lpset :s1
    lpset :s2
    lpset :s3
    lbool :done
  end

  bloom do
    s1 <= in_t {|t| [t.v] if t.which == "s1" }
    s2 <= in_t {|t| [t.v] if t.which == "s2" }
    s3 <= s1
    s3 <= s2
    done <= (s3.pos_sum + 5).gt(25)
  end
end

class TestSum < Test::Unit::TestCase
  def test_sum_simple
    i = SimpleSum.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:in_t, :s1, :s2, :s3, :done].each do |r|
      assert(strat_zero.include? r)
    end
    i.in_t <+ [["s1", 5], ["s1", 3], ["s1", 7],
               ["s2", 5], ["s2", 2]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.in_t <+ [["s2", 7]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)

    i.in_t <+ [["s2", 6]]
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end
end

class SimpleBag
  include Bud

  state do
    lbag :b1
    lbag :b2
    lbag :b_union
    lbag :b_intersect
    lbag :b_sum
    lbool :has_foo
    lbool :done
  end

  bloom do
    b_union <= b1
    b_union <= b2
    b_intersect <= b1.intersect(b2)
    b_intersect <= b2.intersect(b1)
    b_sum <= b1 + b2
    b_sum <= b2 + b1
    has_foo <= b_sum.contains?("foo")
    done <= b_intersect.mult("foo").gt(2)
  end
end

class TestBag < Test::Unit::TestCase
  def test_bag_simple
    i = SimpleBag.new
    assert_equal(2, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    [:b1, :b2, :b_union, :b_intersect, :done].each do |r|
      assert(strat_zero.include? r)
    end

    i.b1 <+ [{"abc" => 2, "def" => 1}, {"abc" => 1}]
    i.tick
    assert_equal([["abc", 2], ["def", 1]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([], i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 1]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(false, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b2 <+ [{"foo" => 1, "def" => 1}]
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 1]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1]], i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 1]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b1 <+ [{"foo" => 2}, {"abc" => 2}]
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 2]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1], ["foo", 1]],
                 i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 3]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b1 <+ [{"foo" => 3}]
    i.b2 <+ [{"foo" => 4}]
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 4]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1], ["foo", 3]],
                 i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 7]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(true, i.done.current_value.reveal)
  end
end

class SimpleSeal
  include Bud

  state do
    lseal :sl
    lmax :m
    lbool :nm_p
    table :foo, [:v]
    table :bar, [:v]
  end

  bootstrap do
    foo <= [[1], [2], [3]]
    bar <= [[4], [5], [6]]
  end

  bloom do
    m <= foo {|t| t.v}
    m <= bar {|t| t.v}
    sl <= m.seal
    nm_p <= sl.safely(:lt_eq, 6)
  end
end

class TestSeal < Test::Unit::TestCase
  def test_seal
    i = SimpleSeal.new
    assert_equal(3, i.strata.length)
    strat_zero = i.stratum_collection_map[0]
    strat_one = i.stratum_collection_map[1]
    [:foo, :bar].each {|r| assert(strat_zero.include? r) }
    [:sl, :nm_p].each {|r| assert(strat_one.include? r) }
    i.tick
    assert(true, i.nm_p.current_value.reveal)
    i.sl <+ Bud::MaxLattice.new(6)
    i.tick
    i.foo <+ [[7]]
    assert_raise(Bud::Error) do
      i.tick
    end
  end
end
