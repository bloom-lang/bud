require './test_common'

class SimpleMax
  include Bud

  state do
    scratch :in_t, [:v]
    lmax :m
    lbool :done
  end

  bloom do
    m <= in_t {|t| t.v}
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
    m1 <= in_t {|t| t.v if t.v % 2 == 0}
    m2 <= in_t {|t| t.v if t.v % 2 == 1}
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

    m1 <= in_t {|t| t.v if t.v % 2 == 0}
    m2 <= in_t {|t| t.v if t.v % 2 == 1}
  end
end

class EmptyMaxMerge
  include Bud

  state do
    lmax :m1
    lmax :m2
  end

  bootstrap do
    m1 <= 5
  end

  bloom do
    m1 <= m2
  end
end

class MaxConstructorImplicit
  include Bud

  state do
    lmax :m1
    lmax :m2
    lmax :m3
  end

  bootstrap do
    m1 <= 5
  end

  bloom do
    m2 <= 6
    m3 <= -7
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
    m <= in_t {|t| t.v}
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
    i.in_t <+ [[5], [10]]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[13]]
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

  def test_embed_max
    i = EmbedMax.new
    %w[in_t t m1 m2].each {|r| assert_equal(0, i.collection_stratum(r))}
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

  def test_max_implicit_cons
    i = MaxConstructorImplicit.new
    i.tick
    assert_equal(Bud::MaxLattice.new(5), i.m1.current_value)
    assert_equal(Bud::MaxLattice.new(6), i.m2.current_value)
    assert_equal(Bud::MaxLattice.new(-7), i.m3.current_value)
  end

  def test_max_over_chn
    src, dst = Array.new(2) { MaxOverChannel.new }
    [src, dst].each {|n| n.run_bg}

    expected_val = Bud::MaxLattice.new
    q = Queue.new
    dst.register_callback(:chn) do |t|
      assert_equal(1, t.length)
      assert_equal(expected_val, t.first.v)
      q.push(true)
    end

    src.sync_do {
      src.do_send <+ [[dst.ip_port]]
    }
    q.pop
    dst.sync_do {
      assert_equal(1, dst.chn_log.length)
      assert_equal(expected_val, dst.chn_log.first.v)
    }

    expected_val = Bud::MaxLattice.new(30)
    src.sync_do {
      [2, 15, 0, 10, 7, 20].each {|i| src.m <+ Bud::MaxLattice.new(i)}
      src.in_t <+ [[16], [30]]
      src.do_send <+ [[dst.ip_port]]
    }
    q.pop
    dst.sync_do {
      assert_equal(1, dst.chn_log.length)
      assert_equal(expected_val, dst.chn_log.first.v)
    }

    [src, dst].each {|n| n.stop}
  end

  def test_merge_type_error
    i = MaxErrors.new
    i.t <+ [["y", self.class]]
    assert_raises(Bud::TypeError) do
      i.tick
    end
  end

  def test_max_equality
    assert(Bud::MaxLattice.new(5) == Bud::MaxLattice.new(5))
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
# links to the end of a previously discovered path, rather than prepending them
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
    link <= [['a', 'b', 1], ['a', 'b', 4],
             ['b', 'c', 1], ['c', 'd', 1],
             ['d', 'e', 1]]
  end

  bloom do
    path <= link
    path <= path.eqjoin(link).pro do |p,l|
      [p[0], l[1], p[2] + l[2]] if p[1] == l[0]
    end
  end
end

# As above, except that we pass a block to eqjoin() directly
class AllPathsImplicitProject
  include Bud

  state do
    lset :link
    lset :path
  end

  bootstrap do
    link <= [['a', 'b', 1], ['a', 'b', 4],
             ['b', 'c', 1], ['c', 'd', 1],
             ['d', 'e', 1]]
  end

  bloom do
    path <= link
    path <= path.eqjoin(link) do |p,l|
      [p[0], l[1], p[2] + l[2]] if p[1] == l[0]
    end
  end
end

# As above, except that we use the eqjoin method
class AllPathsEqJoin
  include Bud

  LinkTuple = Struct.new(:from, :to, :cost)
  PathTuple = Struct.new(:from, :to, :cost)

  state do
    lset :link
    lset :path
  end

  bootstrap do
    link <= [LinkTuple.new('a', 'b', 1),
             LinkTuple.new('a', 'b', 4),
             LinkTuple.new('b', 'c', 1),
             LinkTuple.new('c', 'd', 1),
             LinkTuple.new('d', 'e', 1)]
  end

  bloom do
    path <= link
    path <= path.eqjoin(link, :to => :from) do |p,l|
      PathTuple.new(p.from, l.to, p.cost + l.cost)
    end
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

    # Check that we can also sort directly on the lattice values
    min_cost_sorted = i.min_cost.to_a.sort {|a,b| [a.c, a.from, a.to] <=> [b.c, b.from, b.to]}
    min_cost_sorted = min_cost_sorted.map {|t| [t.from, t.to, t.c.reveal]}
    assert_equal([["c", "d", 5],
                  ["a", "b", 10],
                  ["d", "e", 10],
                  ["a", "c", 15],
                  ["c", "e", 15],
                  ["a", "d", 20],
                  ["b", "c", 20],
                  ["b", "d", 25],
                  ["a", "e", 30],
                  ["b", "e", 35]], min_cost_sorted)
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

  def test_all_paths
    i = AllPathsL.new
    %w[link path].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["b", "c", 1], ["b", "d", 2], ["b", "e", 3], ["c", "d", 1],
                  ["c", "e", 2], ["d", "e", 1]].to_set, i.path.current_value.reveal)

    i.link <+ [['e', 'f', 1]]
    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["a", "f", 5], ["a", "f", 8], ["b", "c", 1], ["b", "d", 2],
                  ["b", "e", 3], ["b", "f", 4], ["c", "d", 1], ["c", "e", 2],
                  ["c", "f", 3], ["d", "e", 1], ["d", "f", 2], ["e", "f", 1]].to_set,
                 i.path.current_value.reveal)
  end

  def test_all_paths_implicit_pro
    i = AllPathsImplicitProject.new
    %w[link path].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["b", "c", 1], ["b", "d", 2], ["b", "e", 3], ["c", "d", 1],
                  ["c", "e", 2], ["d", "e", 1]].to_set, i.path.current_value.reveal)

    i.link <+ [['e', 'f', 1]]
    i.tick
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["a", "f", 5], ["a", "f", 8], ["b", "c", 1], ["b", "d", 2],
                  ["b", "e", 3], ["b", "f", 4], ["c", "d", 1], ["c", "e", 2],
                  ["c", "f", 3], ["d", "e", 1], ["d", "f", 2], ["e", "f", 1]].to_set,
                 i.path.current_value.reveal)
  end

  def test_all_paths_join
    i = AllPathsEqJoin.new
    %w[link path].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.tick
    path_val = i.path.current_value.reveal.map {|v| v.to_a}
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["b", "c", 1], ["b", "d", 2], ["b", "e", 3], ["c", "d", 1],
                  ["c", "e", 2], ["d", "e", 1]].to_set, path_val.to_set)

    i.link <+ [AllPathsEqJoin::LinkTuple.new('e', 'f', 1)]
    i.tick
    path_val = i.path.current_value.reveal.map {|v| v.to_a}
    assert_equal([["a", "b", 1], ["a", "b", 4], ["a", "c", 2], ["a", "c", 5],
                  ["a", "d", 3], ["a", "d", 6], ["a", "e", 4], ["a", "e", 7],
                  ["a", "f", 5], ["a", "f", 8], ["b", "c", 1], ["b", "d", 2],
                  ["b", "e", 3], ["b", "f", 4], ["c", "d", 1], ["c", "e", 2],
                  ["c", "f", 3], ["d", "e", 1], ["d", "f", 2], ["e", "f", 1]].to_set,
                 path_val.to_set)
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
    h <= {"x" => m1, "y" => m1}
    h <= in_t {|t| {t.v => m2}}
    hkeys <= h.key_set
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

class MapBareHashLiteral
  include Bud

  state do
    lmap :m1
  end

  bootstrap do
    m1 <= { "j" => Bud::MaxLattice.new(10),
            "k" => Bud::MaxLattice.new(15) }
  end

  bloom do
    m1 <= { "j" => Bud::MaxLattice.new(20) }
  end
end

class MapAt
  include Bud

  state do
    lmap :m1
    lset :s1
    lset :s2
  end

  bloom do
    s1 <= m1.at("abc", Bud::SetLattice)
    s2 <= m1.at("xyz", Bud::SetLattice)
  end
end

class MapApply
  include Bud

  state do
    lmap :m1
    lmap :m2
    lmap :m3
    lmap :m4
    lset :s1
  end

  bloom do
    m2 <= m1.apply(:pos_sum)
    m3 <= m1.apply_morph(:contains?, 3)
    m4 <= m1.apply(:intersect, Bud::PositiveSetLattice.new([2,3]))
    s1 <= m4.apply(:size).apply(:gt_eq, 2).filter.key_set
  end
end

class MapFromCollection
  include Bud

  state do
    lmap :m1
    scratch :s1
  end

  bloom do
    m1 <= s1 {|t| {t.key => Bud::SetLattice.new([t.val])}}
  end
end

class MapToCollection
  include Bud

  state do
    lmap :m1
    lmap :m2
    table :t1, [:k1, :k2] => [:v1]
  end

  bloom do
    t1 <= m1.to_collection do |k,v|
      [k, k + 1, v + 2] unless k == 99
    end
    t1 <= m2.to_collection do |k,v|
      [k, k + 1, v + 2] unless k == 99
    end
  end
end

class TestMap < MiniTest::Unit::TestCase
  def get_val_for_map(i, r)
    i.send(r).current_value.reveal.map {|k,v| [k, v.reveal]}.sort
  end

  def test_map_simple
    i = SimpleMap.new
    %w[h m1 m2 in_t].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end

    [5, 12, 3].each {|n| i.m1 <+ Bud::MaxLattice.new(n)}
    [3, 4, 5].each {|n| i.m2 <+ Bud::MaxLattice.new(n)}
    i.in_t <+ [["y"], ["z"]]
    i.tick
    assert_equal(12, i.m1.current_value.reveal)
    assert_equal([["x", 12], ["y", 12], ["z", 5]], get_val_for_map(i, :h))
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)

    i.m2 <+ Bud::MaxLattice.new(15)
    i.tick
    assert_equal([["x", 12], ["y", 12], ["z", 5]], get_val_for_map(i, :h))
    assert_equal(15, i.m2.current_value.reveal)
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)

    i.m2 <+ Bud::MaxLattice.new(13)
    i.in_t <+ [["y"], ["z"]]
    i.tick
    assert_equal([["x", 12], ["y", 15], ["z", 15]], get_val_for_map(i, :h))
    assert_equal(15, i.m2.current_value.reveal)
    assert_equal(["x", "y", "z"], i.hkeys.current_value.reveal.sort)
  end

  def test_map_intersect
    i = MapIntersect.new
    %w[m1 m2 m3 m4 done_m3 done_m4].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end
    i.tick
    assert_equal([], get_val_for_map(i, :m3))
    assert_equal([], get_val_for_map(i, :m4))

    i.m1 <+ {"x" => Bud::MaxLattice.new(15)}
    i.m2 <+ {"y" => Bud::MaxLattice.new(20)}
    i.tick
    assert_equal([], get_val_for_map(i, :m3))
    assert_equal([], get_val_for_map(i, :m4))
    assert_equal(false, i.done_m3.current_value.reveal)
    assert_equal(false, i.done_m4.current_value.reveal)

    i.m1 <+ {"y" => Bud::MaxLattice.new(25)}
    i.m2 <+ {"z" => Bud::MaxLattice.new(30)}
    i.tick
    assert_equal([["y", 25]], get_val_for_map(i, :m3))
    assert_equal([["y", 25]], get_val_for_map(i, :m4))
    assert_equal(false, i.done_m3.current_value.reveal)
    assert_equal(false, i.done_m4.current_value.reveal)

    i.m1 <+ {"y" => Bud::MaxLattice.new(31)}
    i.m1 <+ {"z" => Bud::MaxLattice.new(32)}
    i.tick
    assert_equal([["y", 31], ["z", 32]], get_val_for_map(i, :m3))
    assert_equal([["y", 31], ["z", 32]], get_val_for_map(i, :m4))
    assert_equal(true, i.done_m3.current_value.reveal)
    assert_equal(true, i.done_m4.current_value.reveal)
  end

  def test_hash_lit
    i = MapBareHashLiteral.new
    i.tick
    assert_equal([["j", 20], ["k", 15]], get_val_for_map(i, :m1))
  end

  def test_map_at
    i = MapAt.new
    %w[m1 s1 s2].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end
    i.tick
    assert_equal(Set.new, i.s1.current_value.reveal)
    assert_equal(Set.new, i.s2.current_value.reveal)

    i.m1 <+ {"abc" => Bud::SetLattice.new([1])}
    i.m1 <+ {"abc" => Bud::SetLattice.new([2])}
    i.m1 <+ {"xyz" => Bud::SetLattice.new([2,3])}
    i.tick
    assert_equal([1,2].to_set, i.s1.current_value.reveal)
    assert_equal([2,3].to_set, i.s2.current_value.reveal)
  end

  def test_map_apply
    i = MapApply.new
    %w[m1 m2].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end
    i.m1 <+ {"xyz" => Bud::PositiveSetLattice.new([1,2])}
    i.m1 <+ {"xyz" => Bud::PositiveSetLattice.new([3]),
             "abc" => Bud::PositiveSetLattice.new([9, 10])}
    i.tick
    assert_equal([["abc", 19], ["xyz", 6]], get_val_for_map(i, :m2))
    assert_equal([["abc", false], ["xyz", true]], get_val_for_map(i, :m3))
    assert_equal([["abc", [].to_set],
                  ["xyz", [2,3].to_set]], get_val_for_map(i, :m4))
    assert_equal(["xyz"].to_set, i.s1.current_value.reveal)
    i.m1 <+ {"abc" => Bud::PositiveSetLattice.new([3]),
             "xyz" => Bud::PositiveSetLattice.new([4])}
    i.tick
    assert_equal([["abc", 22], ["xyz", 10]], get_val_for_map(i, :m2))
    assert_equal([["abc", true], ["xyz", true]], get_val_for_map(i, :m3))
    assert_equal([["abc", [3].to_set],
                  ["xyz", [2,3].to_set]], get_val_for_map(i, :m4))
    assert_equal(["xyz"].to_set, i.s1.current_value.reveal)
  end

  def test_map_from_collection
    i = MapFromCollection.new
    i.s1 <+ [[5, 10], [7, 9]]
    i.tick
    assert_equal([[5, [10].to_set], [7, [9].to_set]], get_val_for_map(i, :m1))
    i.s1 <+ [[5, 10], [7, 11]]
    i.tick
    assert_equal([[5, [10].to_set], [7, [9, 11].to_set]], get_val_for_map(i, :m1))
  end

  def test_map_to_collection
    i = MapToCollection.new
    i.m1 <+ {10 => Bud::MinLattice.new(5),
             11 => Bud::MinLattice.new(9),
             99 => Bud::MinLattice.new(0)}
    i.tick
    assert_equal([[10, 11, Bud::MinLattice.new(7)],
                  [11, 12, Bud::MinLattice.new(11)]], i.t1.to_a.sort)

    i.m2 <+ {11 => Bud::MinLattice.new(7),
             19 => Bud::MinLattice.new(1)}
    i.tick
    assert_equal([[10, 11, Bud::MinLattice.new(7)],
                  [11, 12, Bud::MinLattice.new(9)],
                  [19, 20, Bud::MinLattice.new(3)]], i.t1.to_a.sort)
  end

  def test_map_equality
    assert(Bud::MapLattice.new(:k => Bud::MaxLattice.new(1),
                               :a => Bud::MaxLattice.new(2)) ==
           Bud::MapLattice.new(:a => Bud::MaxLattice.new(2),
                               :k => Bud::MaxLattice.new(1)))
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

class SetImplicitPro
  include Bud

  state do
    lset :t1
    lset :t2
    lset :t3
  end

  bloom do
    t2 <= t1 {|t| t + 1}
    t3 <= t2 {|t| t + 2}
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
    s3 <= s1.eqjoin(s2)
  end
end

class SetSimpleGroupCnt
  # Groups by first column
  include Bud

  state do
    lset :s1
    lmap :res
  end

  bloom do
    res <= s1.group_count([0])
  end
end

class SetMultipleGroupCnt
  # Groups by first column and third column
  include Bud

  state do
    lset :s1
    lmap :res
  end

  bloom do
    res <= s1.group_count([0, 2])
  end
end

class SetEqjoin
  include Bud

  JoinTuple = Struct.new(:a, :b)

  state do
    lset :s1
    lset :s2
    lset :s3
    lset :s4
    lset :s5
  end

  bloom do
    s3 <= s1.eqjoin(s2, :a => :b) do |x,y|
      [x.a, x.b, y.a]
    end
    s4 <= s1.eqjoin(s2, :a => :b)
    s5 <= s1.eqjoin(s2, :a => :a, :b => :b) {|x,y| x}
  end
end

class SetMethodCompose
  include Bud

  state do
    lset :s1
    lset :s2
    lset :s3
    lset :s4
    lset :s5
    lset :s6
    lset :s7
    lmax :m1
    lbool :done
  end

  bloom do
    s1 <= s2.intersect(s3.intersect(s4))

    s5 <= s2.intersect(s3.intersect(s4))
    s6 <= s4.intersect(s2.intersect(s3))

    s7 <= s2.intersect(s3.merge(s4))
    m1 <= s7.size.merge(s3.size + 1) + 7
    done <= (s7.size.merge(s3.size + 1) + 9).gt_eq(15)
  end
end

class CollectionToSet
  include Bud

  state do
    lset :s1
    table :t1
    table :t2
  end

  bootstrap do
    t1 <= [[3, 4]]
  end

  bloom do
    s1 <= t1
    s1 <= t2
  end
end

class SetToCollection
  include Bud

  state do
    lset :s1
    lset :s2
    table :t1
    table :t2
  end

  bloom do
    t1 <= s1 {|v| [v, 5]}
    t1 <= s2
    t2 <- s1 {|v| [v, 5]}
    t2 <- s2
  end
end

class SetToChannel
  include Bud

  def initialize(addr)
    @serv_addr = addr
    super()
  end

  state do
    lset :s1
    lset :s2
    channel :c1
  end

  bloom do
    c1 <~ s1 {|v| [@serv_addr, v]}
    c1 <~ s2
  end
end

class NotInToLattice
  include Bud

  state do
    lset :s1
    scratch :t1
    table :t2
  end

  bootstrap do
    t2 <= [[1, 1]]
  end

  bloom do
    s1 <= t1.notin(t2, :key => :key)
  end
end

class TestSet < MiniTest::Unit::TestCase
  def test_set_simple
    i = SimpleSet.new
    %w[s1 done in_t].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[2], [3]]
    [5, 6, 7].each {|v| i.s2 <+ [v]}
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[3], [5], [6]]
    i.s2 <+ [12]
    i.tick
    assert_equal(false, i.done.current_value.reveal)
    i.in_t <+ [[12]]
    [2, 14].each {|v| i.s2 <+ [v]}
    i.tick
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_set_implicit_pro
    i = SetImplicitPro.new
    i.t1 <+ [10, 12]
    i.tick
    assert_equal([13, 15].to_set, i.t3.current_value.reveal)
  end

  def test_set_method_compose
    i = SetMethodCompose.new
    %w[s1 s2 s3 s4 s5 s6 s7 m1 done].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end

    [4, 5, 6, 7].each {|v| i.s4 <+ [v]}
    i.tick
    assert_equal(Set.new, i.s1.current_value.reveal)
    assert_equal(Set.new, i.s5.current_value.reveal)
    assert_equal(Set.new, i.s6.current_value.reveal)
    assert_equal(Set.new, i.s7.current_value.reveal)
    assert_equal(8, i.m1.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.s2 <+ [4]
    i.s3 <+ [10]
    i.s3 <+ [11]
    i.tick
    assert_equal(Set.new, i.s1.current_value.reveal)
    assert_equal(Set.new, i.s5.current_value.reveal)
    assert_equal(Set.new, i.s6.current_value.reveal)
    assert_equal([4].to_set, i.s7.current_value.reveal)
    assert_equal(10, i.m1.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.s3 <+ [5]
    i.s3 <+ [6]
    i.s4 <+ [10]
    i.tick
    assert_equal(Set.new, i.s1.current_value.reveal)
    assert_equal(Set.new, i.s5.current_value.reveal)
    assert_equal(Set.new, i.s6.current_value.reveal)
    assert_equal([4].to_set, i.s7.current_value.reveal)
    assert_equal(12, i.m1.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.s3 <+ [4]
    i.tick
    assert_equal([4].to_set, i.s1.current_value.reveal)
    assert_equal([4].to_set, i.s5.current_value.reveal)
    assert_equal([4].to_set, i.s6.current_value.reveal)
    assert_equal([4].to_set, i.s7.current_value.reveal)
    assert_equal(13, i.m1.current_value.reveal)
    assert_equal(true, i.done.current_value.reveal)
  end

  def test_set_simple_groupcnt
    i = SetSimpleGroupCnt.new
    i.tick
    expected = Hash.new(Bud::MaxLattice.new(0))
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a1', 1]]
    expected[['a1']] = Bud::MaxLattice.new(1)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a2', 2]]
    expected[['a2']] = Bud::MaxLattice.new(1)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a1', 3], ['a1', 4]]
    expected[['a1']] = Bud::MaxLattice.new(3)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)
  end

  def test_set_multiple_groupcnt
    i = SetMultipleGroupCnt.new
    i.tick
    expected = Hash.new(Bud::MaxLattice.new(0))
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a1', 'b1', 'c1', 1]]
    expected[['a1', 'c1']] = Bud::MaxLattice.new(1)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a2', 'b2', 'c2', 2]]
    expected[['a2', 'c2']] = Bud::MaxLattice.new(1)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)

    i.s1 <+ [['a1', 'b3', 'c1', 3], ['a1', 'b4', 'c1', 4]]
    expected[['a1', 'c1']] = Bud::MaxLattice.new(3)
    i.tick
    assert_equal(expected, i.res.current_value.reveal)
  end

  def test_set_error_groupcnt
    i = SetSimpleGroupCnt.new
    i.tick

    i.s1 <+ ['a1']
    assert_raises(Bud::TypeError) do
      i.tick
    end

    i = SetSimpleGroupCnt.new
    i.tick
    i.s1 <+ [['a1', 1], []]
    assert_raises(Bud::Error) do
      i.tick
    end
  end

  def test_set_product
    i = SetProduct.new
    i.tick
    assert_equal(Set.new, i.s3.current_value.reveal)

    i.s1 <+ [1]
    i.s1 <+ [2]
    i.tick
    assert_equal(Set.new, i.s3.current_value.reveal)

    i.s2 <+ [3]
    i.tick
    assert_equal([[1,3], [2,3]].to_set, i.s3.current_value.reveal)

    i.s1 <+ [3]
    i.s2 <+ [7]
    i.tick
    assert_equal([[1,3], [1,7], [2,3], [2,7], [3,3], [3,7]].to_set,
                 i.s3.current_value.reveal)
  end

  def test_set_eqjoin
    i = SetEqjoin.new
    i.tick
    assert_equal(Set.new, i.s3.current_value.reveal)
    assert_equal(Set.new, i.s4.current_value.reveal)

    i.s1 <+ [SetEqjoin::JoinTuple.new(1, 2)]
    i.s2 <+ [SetEqjoin::JoinTuple.new(1, 2),
             SetEqjoin::JoinTuple.new(1, 8),
             SetEqjoin::JoinTuple.new(3, 4)]
    i.tick
    assert_equal(Set.new, i.s3.current_value.reveal)
    assert_equal(Set.new, i.s4.current_value.reveal)
    assert_equal([SetEqjoin::JoinTuple.new(1, 2)].to_set,
                 i.s5.current_value.reveal)

    i.s1 <+ [SetEqjoin::JoinTuple.new(4, 7)]
    i.tick
    assert_equal([[4, 7, 3]].to_set, i.s3.current_value.reveal)
    assert_equal([[SetEqjoin::JoinTuple.new(4, 7),
                   SetEqjoin::JoinTuple.new(3, 4)]].to_set,
                 i.s4.current_value.reveal)
    assert_equal([SetEqjoin::JoinTuple.new(1, 2)].to_set,
                 i.s5.current_value.reveal)
  end

  # XXX: Unclear that this is actually the right behavior. The push-based
  # runtime divides the collection into tuples; we then invoke the lattice
  # constructor on each individual n-tuple, which forms an n element set. Then
  # those sets are merged together, losing the tuple structure of the input.
  #
  # This can easily be worked-around (e.g., by wrapping the input tuples in an
  # extra "layer" of brackets), but the right behavior is unclear.
  def test_collection_to_set
    i = CollectionToSet.new
    i.tick
    assert_equal([3, 4].to_set, i.s1.current_value.reveal)

    i.t1 <+ [[5, 6], [1, 4]]
    i.t2 <+ [[10, 11]]
    i.tick
    assert_equal([1, 3, 4, 5, 6, 10, 11].to_set, i.s1.current_value.reveal)
  end

  def test_set_to_collection
    i = SetToCollection.new
    i.s1 <+ [1, 2]
    i.s2 <+ [[5, 10]]
    i.t2 <+ [[1, 5], [2, 6], [4, 9], [5, 10]]
    i.tick
    assert_equal([[1, 5], [2, 5], [5, 10]], i.t1.to_a.sort)
    assert_equal([[1, 5], [2, 6], [4, 9], [5, 10]], i.t2.to_a.sort)
    i.tick
    assert_equal([[2,6], [4, 9]], i.t2.to_a.sort)
  end

  def test_set_to_channel
    sink = SetToChannel.new("")
    sink.run_bg
    source = SetToChannel.new(sink.ip_port)
    source.run_bg

    q = Queue.new
    sink.register_callback(:c1) do |vals|
      vals.to_a.each do |v|
        q.push(v)
      end
    end

    source.sync_do {
      source.s1 <+ ["foo"]
      source.s2 <+ [[sink.ip_port, "bar"]]
    }

    rv = []
    rv << q.pop
    rv << q.pop

    assert_equal([[sink.ip_port, "bar"], [sink.ip_port, "foo"]], rv.sort)

    sink.stop ; source.stop
  end

  def test_collection_notin_to_set
    i = NotInToLattice.new
    i.t1 <+ [[1, 5], [2, 10], [3, 15]]
    i.tick
    assert_equal([2, 3, 10, 15].to_set, i.s1.current_value.reveal)
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

  def test_set_equality
    assert(Bud::SetLattice.new([1,2,3]) == Bud::SetLattice.new([3,2,1]))
    assert(Bud::SetLattice.new([1,2,3]) == Bud::SetLattice.new([2,3,2,1,3,2,1]))
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

class TestSum < MiniTest::Unit::TestCase
  def test_sum_simple
    i = SimpleSum.new
    %w[in_t s1 s2 s3 done].each {|r| assert_equal(0, i.collection_stratum(r))}

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
    lmax :b_sum_size
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
    b_sum_size <= b_sum.size
    has_foo <= b_sum.contains?("foo")
    done <= b_intersect.multiplicity("foo").gt(2)
  end
end

class TestBag < MiniTest::Unit::TestCase
  def test_bag_simple
    i = SimpleBag.new
    %w[b1 b2 b_union b_intersect done].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end

    i.b1 <+ {"abc" => 2, "def" => 1}
    i.b1 <+ {"abc" => 1}
    i.tick
    assert_equal([["abc", 2], ["def", 1]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([], i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 1]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(3, i.b_sum_size.current_value.reveal)
    assert_equal(false, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b2 <+ {"foo" => 1, "def" => 1}
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 1]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1]], i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 1]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(5, i.b_sum_size.current_value.reveal)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b1 <+ {"foo" => 2}
    i.b1 <+ {"abc" => 2}
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 2]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1], ["foo", 1]],
                 i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 3]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(7, i.b_sum_size.current_value.reveal)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(false, i.done.current_value.reveal)

    i.b1 <+ {"foo" => 3}
    i.b2 <+ {"foo" => 4}
    i.tick
    assert_equal([["abc", 2], ["def", 1], ["foo", 4]],
                 i.b_union.current_value.reveal.to_a.sort)
    assert_equal([["def", 1], ["foo", 3]],
                 i.b_intersect.current_value.reveal.to_a.sort)
    assert_equal([["abc", 2], ["def", 2], ["foo", 7]],
                 i.b_sum.current_value.reveal.to_a.sort)
    assert_equal(11, i.b_sum_size.current_value.reveal)
    assert_equal(true, i.has_foo.current_value.reveal)
    assert_equal(true, i.done.current_value.reveal)
  end
end

module LatticeMod1
  state do
    lmax :m1
    lset :s1
  end
end

class LatticeModParent
  include Bud
  import LatticeMod1 => :x1
  import LatticeMod1 => :x2

  bootstrap do
    x1.m1 <= 0
    x2.m1 <= 4
  end

  state do
    lmax :m1
    lmax :cnt
    lset :s1
  end

  bloom do
    m1 <= x1.m1 + 3
    m1 <= x2.m1
    s1 <= x1.s1.merge(Bud::SetLattice.new([1,2,3]))
    s1 <= x2.s1
    cnt <= x1.s1.size
  end
end

class TestLatticesWithModules < MiniTest::Unit::TestCase
  def test_lattice_module_simple
    i = LatticeModParent.new
    %w[m1 cnt s1 x1.m1 x1.s1 x2.m1 x2.s1].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.x1.m1 <+ Bud::MaxLattice.new(3)
    i.s1 <+ Bud::SetLattice.new([4])
    i.tick
    assert_equal(6, i.m1.current_value.reveal)
    assert_equal(0, i.cnt.current_value.reveal)
    assert_equal([1,2,3,4], i.s1.current_value.reveal.sort)

    i.x1.s1 <+ Bud::SetLattice.new([2, 6])
    i.x2.s1 <+ Bud::SetLattice.new([2, 5])
    i.x2.m1 <+ Bud::MaxLattice.new(5)
    i.tick
    assert_equal(6, i.m1.current_value.reveal)
    assert_equal(2, i.cnt.current_value.reveal)
    assert_equal([1,2,3,4,5,6], i.s1.current_value.reveal.sort)
  end
end

class RescanLattice
  include Bud

  state do
    scratch :s1, [:a, :b]
    scratch :s1_at_next, s1.schema
    scratch :s2, [:v]
    scratch :s2_at_next, s2.schema
    lset :set_input
    lset :set_derived
    lset :set_as_tuple
  end

  bloom do
    set_derived <= set_input {|i| i + 1}
    s1 <= set_derived {|i| [i, i + 2]}
    s1_at_next <+ set_derived {|i| [i, i + 2]}

    set_as_tuple <= set_derived {|i| [i]}
    s2 <= set_as_tuple
    s2_at_next <+ set_as_tuple
  end
end

class RescanLatticeTests < MiniTest::Unit::TestCase
  def test_rescan_lattice
    i = RescanLattice.new
    i.set_input <+ Bud::SetLattice.new([5])
    i.tick
    assert_equal([[6, 8]], i.s1.to_a.sort)
    assert_equal([], i.s1_at_next.to_a.sort)
    assert_equal([[6]], i.s2.to_a.sort)
    assert_equal([], i.s2_at_next.to_a.sort)
    i.set_input <+ Bud::SetLattice.new([4])
    i.tick
    assert_equal([[5, 7], [6, 8]], i.s1.to_a.sort)
    assert_equal([[6, 8]], i.s1_at_next.to_a.sort)
    assert_equal([[5], [6]], i.s2.to_a.sort)
    assert_equal([[6]], i.s2_at_next.to_a.sort)
    i.tick
    assert_equal([[5, 7], [6, 8]], i.s1.to_a.sort)
    assert_equal([[5, 7], [6, 8]], i.s1_at_next.to_a.sort)
    assert_equal([[5], [6]], i.s2.to_a.sort)
    assert_equal([[5], [6]], i.s2_at_next.to_a.sort)
  end
end

class LatticeEmbedJoin
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    lmax :m1
    lmax :m2
  end

  bloom do
    t1 <= (t2 * t3).lefts {|x| [x.key, m1]}
    m1 <= m2
  end
end

class LatticeEmbedAgg
  include Bud

  state do
    table :t1, [:v1, :v2] => [:embed_v]
    table :t2, [:v1, :v2]
    lset :s1
  end

  bloom do
    t1 <= t2.group([:v1], max(:v2)) do |t|
      t + [s1]
    end
  end
end

class LatticeEmbedCollExpr
  include Bud

  state do
    table :t1
    lmin :m1
    lmin :m2
  end

  bloom do
    t1 <= [["x", m1 + 1], ["y", m2 + 2]]
    m1 <= m2 + 5
  end
end

class TestLatticeEmbedDeltas < MiniTest::Unit::TestCase
  def test_join_deltas
    i = LatticeEmbedJoin.new
    %w[t1 t2 t3 m1 m2].each {|r| assert_equal(0, i.collection_stratum(r))}
    depends = i.t_depends.map {|t| [t.lhs, t.body, t.in_body, t.nm]}.to_set
    assert_equal([["t1", "t2", false, false], ["t1", "t3", false, false],
                  ["t1", "m1", true, false], ["m1", "m2", false, false]].to_set,
                 depends)
    i.t2 <+ [[5, 10]]
    i.t3 <+ [[10, 20]]
    i.m1 <+ Bud::MaxLattice.new(5)
    i.tick
    assert_equal([[5, Bud::MaxLattice.new(5)]], i.t1.to_a)
    i.m2 <+ Bud::MaxLattice.new(12)
    i.tick
    assert_equal([[5, Bud::MaxLattice.new(12)]], i.t1.to_a)
    i.m1 <+ Bud::MaxLattice.new(14)
    i.tick
    assert_equal([[5, Bud::MaxLattice.new(14)]], i.t1.to_a)
  end

  def test_agg_deltas
    i = LatticeEmbedAgg.new
    %w[t2].each {|r| assert_equal(0, i.collection_stratum(r))}
    %w[t1].each {|r| assert_equal(1, i.collection_stratum(r))}
    depends = i.t_depends.map {|t| [t.lhs, t.body, t.in_body, t.nm]}.to_set
    assert_equal([["t1", "t2", false, true], ["t1", "s1", true, true]].to_set,
                 depends)
    i.t2 <+ [[5, 10], [5, 11], [6, 12]]
    i.s1 <+ Bud::SetLattice.new([7])
    i.tick
    assert_equal([[5, 11, Bud::SetLattice.new([7])],
                  [6, 12, Bud::SetLattice.new([7])]], i.t1.to_a.sort)
    i.s1 <+ Bud::SetLattice.new([9])
    i.t2 <+ [[7, 19]]
    i.tick
    assert_equal([[5, 11, Bud::SetLattice.new([7, 9])],
                  [6, 12, Bud::SetLattice.new([7, 9])],
                  [7, 19, Bud::SetLattice.new([7, 9])]], i.t1.to_a.sort)
  end

  def test_coll_expr_deltas
    i = LatticeEmbedCollExpr.new
    %w[t1 m1 m2].each {|r| assert_equal(0, i.collection_stratum(r))}

    i.m2 <+ Bud::MinLattice.new(5)
    i.m2 <+ Bud::MinLattice.new(4)
    i.tick
    assert_equal(9, i.m1.current_value.reveal)
    assert_equal([["x", Bud::MinLattice.new(10)], ["y", Bud::MinLattice.new(6)]],
                 i.t1.to_a.sort)

    i.m2 <+ Bud::MinLattice.new(3)
    i.tick
    assert_equal(8, i.m1.current_value.reveal)
    assert_equal([["x", Bud::MinLattice.new(9)], ["y", Bud::MinLattice.new(5)]],
                 i.t1.to_a.sort)
  end
end

class ChannelWithLatticeRhs
  include Bud

  state do
    channel :chn, [:@addr] => [:v]
    table :chn_at_next, chn.schema
    table :chn_log, chn.schema
    lbool :do_send
  end

  bloom do
    chn <~ do_send.when_true {
      [[ip_port, 'hello, world!']]
    }
    chn_at_next <+ do_send.when_true {
      [[ip_port, 'hello, world!']]
    }
    chn_log <= chn
  end
end

class TestChannelWithLatticeRhs < MiniTest::Unit::TestCase
  def test_channel_rhs
    i = ChannelWithLatticeRhs.new
    i.run_bg
    i.sync_do {
      i.do_send <+ Bud::BoolLattice.new(true)
    }
    i.delta(:chn)
    i.sync_do {
      assert_equal([[i.ip_port, 'hello, world!']], i.chn_log.to_a)
      assert_equal([[i.ip_port, 'hello, world!']], i.chn_at_next.to_a)
    }
    i.stop
  end
end

class PendingLatticeMerge
  include Bud

  state do
    lmap :m1
    scratch :s1
  end

  bloom do
    m1 <+ s1 {|t| {t.key => t.val}}
  end
end

class TestPendingLatticeMerge < MiniTest::Unit::TestCase
  def test_pending_merge
    i = PendingLatticeMerge.new
    i.s1 <+ [[5, Bud::MaxLattice.new(10)]]
    i.tick
    assert_equal(Bud::MapLattice.new, i.m1.current_value)
    i.tick
    assert_equal(Bud::MapLattice.new(5 => Bud::MaxLattice.new(10)),
                 i.m1.current_value)
  end
end

class TestLatticeHashing < MiniTest::Unit::TestCase
  def test_lattice_hash_eq
    m1 = Bud::MapLattice.new({:foo => Bud::MaxLattice.new(3)})
    m2 = Bud::MapLattice.new({:foo => Bud::MaxLattice.new(3)})
    assert(m1 == m2)
    assert(m1.eql? m2)
    assert_equal(m1.hash, m2.hash)

    h = {}
    h[m1] = 5
    h[m2] = 10
    assert_equal(1, h.size)
    assert_equal({Bud::MapLattice.new({:foo => Bud::MaxLattice.new(3)}) => 10}, h)
  end
end

class BootstrapNoRules
  include Bud

  state do
    lmax :cloq
    lmax :other
  end

  bootstrap do
    cloq <= Bud::MaxLattice.new(3)
  end

  bloom do
    other <= other
  end
end

class TestLatticeBootstrapNoRules < MiniTest::Unit::TestCase
  def test_bootstrap
    b = BootstrapNoRules.new
    b.tick
    assert_equal(3, b.cloq.current_value.reveal)
    b.tick
  end
end

class Bug290
  include Bud

  state do
    lmax :cloq
    table :truth, []=>[:val]
  end

  bootstrap do
    truth <+ [[true]]
    cloq <+ Bud::MaxLattice.new(0)
  end

  bloom do
    cloq <+ truth {|t| cloq + 1}
  end
end

class Bug290Test < MiniTest::Unit::TestCase
  def test_no_bug
    b = Bug290.new
    5.times do |i|
      b.tick
      assert_equal(Bud::MaxLattice.new(i), b.cloq.current_value)
    end
  end
end

class MarshalNestedLattices
  include Bud

  state do
    loopback :lb
    table :stash
  end

  bootstrap do
    lb <~ [ ["localhost", [Bud::MaxLattice.new(0)]] ]
  end

  bloom do
    stash <= lb
  end
end

class TestMarshalNestedLattices < MiniTest::Unit::TestCase
  def test_marshal
    b = MarshalNestedLattices.new
    q = Queue.new
    b.register_callback(:lb) do |t|
      q.push(true)
    end
    b.run_bg
    q.pop
    b.stop

    assert_equal([["localhost", [Bud::MaxLattice.new(0)]]], b.stash.to_a)
  end
end
