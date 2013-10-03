require './test_common'

class PushTests < MiniTest::Unit::TestCase
  class SimplePush
    include Bud
    state do
      table :r1
      table :r2
    end
    bloom do 
      r2 <= r1
    end
  end
  def test_simple
    p = SimplePush.new
    p.r1 <+ [[:a,1]]
    p.tick
    assert_equal([[:a,1]], p.r2.to_a)
    p.tick
    assert_equal([[:a,1]], p.r2.to_a)
  end
  
  class PushThru
    include Bud
    state do
      table :r1
      table :r2
      scratch :sc
    end
    bloom do
      r1 <= sc; r2 <= r1
    end
  end
  def test_push_thru
    p = PushThru.new
    p.tick
    assert_equal([], p.r2.to_a)
    p.sc <+ [[:a,1]]
    p.tick
    assert_equal([[:a,1]], p.r1.to_a)
    assert_equal([[:a,1]], p.r2.to_a)
  end
  
  class PushJoin
    include Bud
    state do
      table :r1
      table :r2
      table :t1
    end
    bloom do
      t1 <= (r1*r2).pairs(:key=>:key){|x,y| [x[0], x[1]+y[1]]}
    end
  end
  def test_push_join
    p = PushJoin.new
    p.tick
    p.r1 <+ [[:a,1]]
    p.r2 <+ [[:a,2]]
    p.tick
    assert_equal([[:a,3]], p.t1.to_a)
  end
  
  class PushTwoJoins
    include Bud
    state do
      table :r1
      table :r2
      table :r3
      table :t1
    end
    bloom do
      t1 <= (r1*r2*r3).pairs(r1.key=>r2.key, r2.key=>r3.key){|x,y,z| [x.key, x.val+y.val+z.val]}
    end
  end
  def test_two_joins
    p = PushTwoJoins.new
    p.r1 <+ [[:a,1]]
    p.r2 <+ [[:a,2]]
    p.r3 <+ [[:a,3]]
    p.tick
    assert_equal([[:a,6]], p.t1.to_a)
  end
  class PushCartesian
    include Bud
    state do
      table :r1
      table :r2
      table :t1
    end
    bloom do
      t1 <= (r1*r2).pairs{|x,y| [x.key, y.key]}
    end
  end
  def test_cartesian
    p = PushCartesian.new
    p.tick
    p.r1 <+ [['a',1],['b',3]]
    p.r2 <+ [['a',2]]
    p.tick
    p.tick
    assert_equal([['a','a'],['b','a']], p.t1.to_a.sort)
  end
  class PushRecursion
    include Bud
    state do
      table :link, [:src, :dest]
      table :path, [:src, :dest]
    end
    bloom do
      path <= link
      path <= (link*path).pairs(:dest=>:src){|l,p| [l.src, p.dest]}
    end
  end
  def test_recursion
    p = PushRecursion.new
    p.tick
    p.link <+ [[1,2],[2,3],[3,4],[6,7],[2,7]]
    p.tick
    assert_equal([[1,2],[1,3],[1,4],[1,7],[2,3],[2,4],[2,7],[3,4],[6,7]], p.path.to_a.sort)
  end
  class PushGBTest
    include Bud
    state do
      table :groupin, [:key,:val]
      table :result
    end
    bloom do
      result <= groupin.group([:key], sum(:val))
    end
  end
  def test_gb
    p = PushGBTest.new
    p.tick
    p.groupin <+ [[1,1],[1,2],[2,3],[2,4]]
    p.tick
    assert_equal([[1,3],[2,7]], p.result.to_a.sort)
  end
  class PushArgAggTest
    include Bud
    state do
      scratch :r1, [:key, :val]
      table :result, [:key,:val]
    end
    bloom do
      result <= r1.argagg(:min, [:val], :key)
    end
  end
  def test_argagg
    p = PushArgAggTest.new
    p.r1 <+ [[1,'a'],[1,'b'],[2,'b'],[2,'a']]
    p.tick
    assert_equal([[1,'a'],[1,'b']], p.result.to_a.sort)
  end
  class PushArgAggTestNoGroup
    include Bud
    state do
      table :r1, [:key, :val]
      table :result, [:key,:val]
    end
    bloom do
      result <= r1.argagg(:min, [], :key)
    end
  end
  def test_argagg_nogroup
    p = PushArgAggTestNoGroup.new
    p.tick
    p.r1 <+ [[1,'a'],[1,'b'],[2,'b'],[2,'a']]
    p.tick
    assert([[1,'a'],[1,'b']].sort == p.result.to_a.sort)
  end
  class PushInspected
    include Bud
    state do
      table :r1
      table :result, [:str]
    end
    bloom do
      result <= r1.inspected
    end
  end
  def test_inspected
    p = PushInspected.new
    p.tick
    p.r1 <+ [[1,1],[2,2]]
    p.tick
    assert_equal([[ "[1, 1]" ],[ "[2, 2]" ]], p.result.to_a.sort)
  end
  class PushStrata
    include Bud
    state do
      table :link, [:src, :dest]
      table :path, [:src, :dest]
      table :win
      table :result
    end
    bloom do
      path <= link
      path <= (link*path).pairs(:dest=>:src){|l,p| [l.src, p.dest]}
      result <= win {|w| w unless path.include? [3,7]}
    end
  end
  def test_strata
    p = PushStrata.new
    p.tick
    p.link <+ [[1,2],[2,3],[3,4],[6,7],[2,7]]
    p.win <+ [[1,1]]
    p.tick
    assert_equal([[1,2],[1,3],[1,4],[1,7],[2,3],[2,4],[2,7],[3,4],[6,7]], p.path.to_a.sort)
    assert_equal([[1,1]], p.result.to_a)
  end 

  class BudtimeRecompute
    include Bud
    state do
      table :t1, [:x]
      table :t2, [:tstamp, :x]
    end
    bloom do
      t2 <= t1 {|t| [budtime, t.x]}
    end
  end

  def test_budtime
    b = BudtimeRecompute.new
    b.t1 <+ [["foo"]]
    b.tick
    assert_equal(1, b.t2.to_a.length)
    b.tick
    assert_equal(2, b.t2.to_a.length)
    b.t1 <+ [["bar"]]
    b.tick
    assert_equal(4, b.t2.to_a.length)
  end

  class DeleteRescan
    include Bud

    state do
      table :src, [:str]
      table :sink, src.schema
      scratch :dummy, sink.schema
      scratch :event_src, src.schema
    end

    bloom do
      sink <= src
      sink <= event_src
      dummy <= sink
    end
  end

  def test_delete_rescan
    b = DeleteRescan.new
    b.src <+ [["v1"], ["v2"]]
    b.event_src <+ [["v3"]]
    b.tick
    assert_equal([["v1"], ["v2"], ["v3"]], b.sink.to_a.sort)
    assert_equal([["v1"], ["v2"], ["v3"]], b.dummy.to_a.sort)

    b.sink <- [["v1"], ["v3"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)
    assert_equal([["v1"], ["v2"]], b.dummy.to_a.sort)

    b.src <- [["v1"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)
    assert_equal([["v1"], ["v2"]], b.dummy.to_a.sort)

    b.sink <- [["v1"], ["v2"]]
    b.tick
    assert_equal([["v2"]], b.sink.to_a.sort)
    assert_equal([["v2"]], b.dummy.to_a.sort)
  end

  class DeleteRescanNM
    include Bud

    state do
      table :src, [:str]
      table :src_del, [:str]
      table :xyz, [:str]
      table :abc, [:str]
      table :sink, src.schema
      scratch :dummy, sink.schema
    end

    bloom do
      src_del <= xyz.notin(abc)
      sink <= src.notin(src_del, :str => :str)
      dummy <= sink
    end
  end

  def test_delete_rescan_nm
    b = DeleteRescanNM.new
    b.src <+ [["v1"], ["v2"], ["v4"]]
    b.src_del <+ [["v1"], ["v3"]]
    b.tick
    assert_equal([["v2"], ["v4"]], b.sink.to_a.sort)
    assert_equal([["v2"], ["v4"]], b.dummy.to_a.sort)

    b.sink <- [["v2"]]
    b.tick
    assert_equal([["v2"], ["v4"]], b.sink.to_a.sort)
    assert_equal([["v2"], ["v4"]], b.dummy.to_a.sort)

    b.src_del <+ [["v2"]]
    b.tick
    assert_equal([["v2"], ["v4"]], b.sink.to_a.sort)
    assert_equal([["v2"], ["v4"]], b.dummy.to_a.sort)

    b.sink <- [["v2"]]
    b.tick
    assert_equal([["v4"]], b.sink.to_a.sort)
    assert_equal([["v4"]], b.dummy.to_a.sort)
  end

  # Identical to DeleteRescan, except that "sink" does not appear on the RHS of
  # any rules. Hence it doesn't get a scanner, so the previous invalidation
  # coding didn't operate correctly.
  class DeleteRescanOrphan
    include Bud

    state do
      table :src, [:str]
      table :sink, src.schema
    end

    bloom do
      sink <= src
    end
  end

  def test_delete_rescan_orphan
    b = DeleteRescanOrphan.new
    b.src <+ [["v1"], ["v2"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.sink <- [["v1"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.src <- [["v1"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.sink <- [["v1"], ["v2"]]
    b.tick
    assert_equal([["v2"]], b.sink.to_a.sort)
  end

  class DeleteRescanPending
    include Bud

    state do
      table :src, [:str]
      table :sink, src.schema
    end

    bloom do
      sink <+ src
    end
  end

  def test_delete_rescan_pending
    b = DeleteRescanPending.new

    # Check that the <+ rule is not regarded as non-monotonic. We should
    # probably move this into a separate test.
    deps = b.t_depends.select {|d| d.lhs == "sink" and d.body == "src"}
    assert_equal(1, deps.size)
    assert_equal(false, deps.first.nm)

    b.src <+ [["v1"], ["v2"]]
    b.tick
    assert_equal([], b.sink.to_a)
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.sink <- [["v1"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.src <- [["v1"]]
    b.sink <- [["v1"]]
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)
    b.tick
    assert_equal([["v1"], ["v2"]], b.sink.to_a.sort)

    b.sink <- [["v1"]]
    b.tick
    assert_equal([["v2"]], b.sink.to_a.sort)
  end
end
