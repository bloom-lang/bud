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

  def ntest_embed_max
    i = EmbedMax.new
    %w[in_t t m1 m2].each {|r| assert_equal(0, i.collection_stratum(r))}
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

  def test_empty_max
    i = EmptyMaxMerge.new
    i.tick
  end
end

# Based on Example 3.1 in "Monotonic Aggregation in Deductive Databases" (Ross
# and Sagiv, PODS'92). Unlike in R&S, we don't need to compute min_cost as a
# separate relation, although we do so for testing purposes.
class ShortestPathsL
  include Bud

  state do
    table :link, [:from, :to, :c]
    table :path, [:from, :to, :next_hop] => [:c]
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
    table :path, [:from, :to, :next_hop] => [:c]
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
    table :path, [:from, :to, :next_hop] => [:c]
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

class TestGraphPrograms < MiniTest::Unit::TestCase
  def test_spath_simple
    i = ShortestPathsL.new
    %w[link path min_cost].each {|r| assert_equal(0, i.collection_stratum(r))}
    i.link <+ [["a", "b", 11],
               ["a", "b", 10],
               ["a", "c", 15],
               ["b", "c", 20],
               ["b", "c", 21],
               ["b", "d", 30],
               ["c", "d", 5],
               ["d", "e", 10]]
    i.tick
    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next_hop, t.c.reveal]}
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

    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next_hop, t.c.reveal]}
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
    %w[link path min_cost].each {|r| assert_equal(0, i.collection_stratum(r))}
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

    path_r = i.path.to_a.map {|t| [t.from, t.to, t.next_hop, t.c.reveal]}
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
    %w[link path max_cap].each {|r| assert_equal(0, i.collection_stratum(r))}

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
