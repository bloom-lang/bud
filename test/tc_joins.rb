require './test_common'

class StarJoin
  include Bud
  state do
    table :r1
    table :r2, [:key] => [:vat]
    table :r3
    table :r5
    table :r51
    table :r52
    table :r7
    table :r8
    table :r9
    table :r11
  end

  bootstrap do
    r1 <= [[1,1]]
    r2 <= [[1,2],[3,4]]
  end

  bloom do
    r3 <= (r1*r2).pro {|r,s| [s.vat, r.key]}
    r5 <= (r1*r2).pairs(:val => :key) {|r,s| [r.key, s.vat]}
    r51 <= (r1*r2).pairs([r1.val,r2.key]) {|r,s| [r.key, s.vat]}
    r52 <= (r1*r2).pairs(r2.key => r1.val) {|r,s| [r.key, s.vat]}
    r7 <= (r1*r2).matches {|r,s| [r.key, s.vat]}
    r8 <= (r1*r1*r2*r2).matches {|r1,r2,s1,s2| [r1.key, s1.vat]}
    r9 <= (r1*r2).lefts(:val => :key)
    r11 <= (r1*r2).rights(:val => :key)
  end
end

class StarJoin3
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :r1, [:k4] => [:v4]
    table :r2, [:k5] => [:v5]
    table :r3, [:k6] => [:v6]
    table :r4, [:k6] => [:v7]
    table :r5, [:k6] => [:v8]
    table :t4, [:k1,:v1,:k2,:v2,:k3,:v3]
    table :t5, [:k1,:v1,:k2,:v2,:k3,:v3]
    table :t6, [:k1,:v1,:k2,:v2,:k3,:v3]
  end

  bootstrap do
    t1 <= [['A', 'B']]
    t2 <= [[3,4]]
    t3 <= [['A', 'Y']]
    r1 <= [['A', 'B']]
    r2 <= [[3,4]]
    r3 <= [['A', 'Y']]
    r4 <= [['A', 'X']]
    r5 <= [['A', 'Z']]
  end

  bloom do
    t4 <= (r1 * r2 * r3).pairs(:k4 => :k6) {|r,s,t| r.to_a + s.to_a + t.to_a}
    t5 <= (t1 * t2 * t3).combos(t1.key => t3.key) {|r,s,t| r.to_a + s.to_a + t.to_a}
    t6 <= (r3 * r4 * r5).matches {|r,s,t| r.to_a + s.to_a + t.to_a}
  end
end

class MixedAttrRefs
  include Bud
  state do
    table :r1
    table :r2
    table :r3
  end

  bloom do
    r3 <= (r1*r2).pairs(:key => r2.val)
  end
end

class MissingAttrRefs
  include Bud
  state do
    table :r1
    table :r2
    table :r3
  end

  bloom do
    r3 <= (r1*r2).pairs(:i_dont_exist => :ha)
  end
end

class IllegalAttrRefs
  include Bud
  state do
    table :r1
    table :r2
    table :r3
  end

  bloom do
    r3 <= (r1*r2).pairs("key" => "val")
  end
end

class AmbiguousAttrRefs
  include Bud
  state do
    table :r1
    table :r2
    table :r3
  end

  bloom do
    temp :r4 <= (r1*r2*r3).pairs(:key => :val)
  end
end

class UnJoinedTableRef
  include Bud
  state do
    table :joined1
    table :joined2
    table :joined3
    table :unjoined
  end
  bootstrap do
    joined1 << [1,1]
    joined2 << [2,2]
    joined3 << [3,3]
    unjoined << [4,4]
  end
  bloom do
    temp :r4 <= (joined1*joined2*joined3).pairs(unjoined.key=>joined2.val)
  end
end


class CombosBud
  include Bud

  state do
    table :r, [:x, :y1]
    table :s_tab, [:x, :y1]
    table :t, [:x, :y1]
    table :mismatches, [:x, :y1]
    scratch :simple_out, [:x, :y1, :y2]
    scratch :match_out, [:x, :y1, :y2]
    scratch :chain_out, [:x1, :x2, :x3, :y1, :y2, :y3]
    scratch :flip_out, [:x1, :x2, :x3, :y1, :y2, :y3]
    scratch :nat_out, [:x1, :x2, :x3, :y1, :y2, :y3]
    scratch :loj_out, [:x1, :x2, :y1, :y2]
    scratch :newtab_out, [:x1, :x2, :x3, :y1, :y2, :y3]
  end

  bootstrap do
    r << ['a', 1]
    r << ['b', 1]
    r << ['b', 2]
    r << ['c', 1]
    r << ['c', 2]
    s_tab << ['a', 1]
    s_tab << ['b', 2]
    s_tab << ['c', 1]
    s_tab << ['c', 2]
    t << ['a', 1]
    t << ['z', 1]
    mismatches << ['a', 1]
    mismatches << ['v', 1]
    mismatches << ['z', 1]
  end

  bloom do
    simple_out <= (r * s_tab).pairs(:x => :x) {|t1,t2| [t1.x, t1.y1, t2.y1] }

    match_out <= (r * s_tab).pairs(:x => :x, :y1 => :y1) { |t1,t2| [t1.x, t1.y1, t2.y1] }

    chain_out <= (r * s_tab * t).combos(r.x => s_tab.x, s_tab.x => t.x) { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y1, t2.y1, t3.y1] }

    nat_out <= (r * s_tab * t).matches { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y1, t2.y1, t3.y1] }

    newtab_out <= (r * s_tab * t).combos(r.x => s_tab.x, s_tab.x => t.x) { |a,b,c| [a.x, b.x, c.x, a.y1, b.y1, c.y1] }

    loj_out <= (mismatches * s_tab).outer(:x => :x) { |t1, t2| [t1.x, t2.x, t1.y1, t2.y1] }
  end
end

# Check that assignment operators within nested blocks aren't confused for a
# join alias -- Issue #82.
class BlockAssign
  include Bud

  state do
    table :num, [:num]
  end

  bloom do
    num <= (1..5).map do |i|
      foo = i
      [foo]
    end
  end
end

# Check that "<<" within a nested block isn't confused for a Bloom op (#84).
class BlockAppend
  include Bud

  state do
    table :num, [:num]
  end

  bloom do
    num <= (1..5).map do |i|
      foo = []
      foo << i
    end
  end
end

class RenameJoin
  include Bud

  state do
    table :foo
  end

  bootstrap do
    foo << ['a', 1]
  end

  bloom do
    temp :out <= (foo.rename(:floo, [:c1]=>[:c2]) * foo).lefts(:c1 => :key)
    temp :out2 <= (foo.rename(:floo2, [:c1] => [:c2]) * foo).rights([floo2.c1, foo.key])
  end
end

class PartlyQualifiedCombo
  include Bud
  state do
    table :arr
    table :ess
    table :tee
    table :result1, [:a, :b, :c]
    table :result2, [:a, :b, :c]
  end

  bootstrap do
    arr << [1, 2]
    ess << [1, 3]
    tee << [5, 6]
  end

  bloom do
    result1 <= (tee * arr * ess).combos(arr.key => ess.key)
    result2 <= (arr * ess * tee).combos(arr.key => ess.key)
  end
end

class Issue313
  include Bud

  state do
    scratch :explicit_tc, [:from, :to]
    scratch :use_tiebreak, [:from, :to]
    scratch :sem_hist, [:from, :to]
    scratch :result, [:from, :to]
  end

  bloom do
    result <= (sem_hist * use_tiebreak * explicit_tc).combos(sem_hist.from => use_tiebreak.from,
                                                             sem_hist.to => explicit_tc.from,
                                                             sem_hist.from => explicit_tc.to) do |s,t,e|
      [s.to, t.to]
    end
  end
end

class TestJoins < MiniTest::Unit::TestCase
  def test_combos
    program = CombosBud.new
    program.tick
    simple_outs = program.simple_out
    assert_equal(7, simple_outs.length)
    assert_equal(1, simple_outs.count { |t| t[0] == 'a'})
    assert_equal(2, simple_outs.count { |t| t[0] == 'b'})
    assert_equal(4, simple_outs.count { |t| t[0] == 'c'})
  end

  def test_secondary_join_predicates
    program = CombosBud.new
    program.tick
    match_outs = program.match_out
    assert_equal(4, match_outs.length)
    assert_equal(1, match_outs.count { |t| t[0] == 'a'})
    assert_equal(1, match_outs.count { |t| t[0] == 'b'})
    assert_equal(2, match_outs.count { |t| t[0] == 'c'})
  end

  def test_3_joins
    program = CombosBud.new
    program.tick
    chain_outs = program.chain_out.to_a
    assert_equal(1, chain_outs.length)
    assert_equal(1, program.nat_out.length)
    assert_equal(chain_outs, program.newtab_out.to_a)
  end

  def test_block_assign
    program = BlockAssign.new
    program.tick
    assert_equal([[1],[2],[3],[4],[5]], program.num.to_a.sort)
  end

  def test_block_append
    program = BlockAppend.new
    program.tick
    assert_equal([[1],[2],[3],[4],[5]], program.num.to_a.sort)
  end

  def test_left_outer_join
    program = CombosBud.new
    program.tick
    loj_outs = program.loj_out
    assert_equal(3, loj_outs.length)
    assert_equal([["a", "a", 1, 1], ["v", nil, 1, nil], ["z", nil, 1, nil]], loj_outs.to_a.sort)
  end

  def test_star_join
    program = StarJoin.new
    program.tick
    assert_equal([[2,1],[4,1]], program.r3.to_a.sort)
    assert_equal(program.r5.to_a.sort, program.r51.to_a.sort)
    assert_equal(program.r5.to_a.sort, program.r52.to_a.sort)
    assert_equal([[1,2]], program.r5.to_a.sort)
    assert_equal([[1,2]], program.r7.to_a.sort)
    assert_equal([[1,2]], program.r8.to_a.sort)
    assert_equal([[1,1]], program.r9.to_a.sort)
    assert_equal([[1,2]], program.r11.to_a.sort)
  end

  def test_star_join3
    program = StarJoin3.new
    program.tick
    assert_equal([['A','B',3,4,'A','Y']], program.t4.to_a)
    assert_equal(program.t4.to_a, program.t5.to_a)
    assert_equal([["A", "Y", "A", "X", "A", "Z"]], program.t6.to_a)
  end

  def test_bad_star_joins
    p1 = MixedAttrRefs.new
    assert_raises(Bud::CompileError) {p1.tick}
    p2 = MissingAttrRefs.new
    assert_raises(Bud::CompileError) {p2.tick}
    p3 = IllegalAttrRefs.new
    assert_raises(Bud::CompileError) {p3.tick}
    p4 = AmbiguousAttrRefs.new
    assert_raises(Bud::CompileError) {p4.tick}
    p5 = UnJoinedTableRef.new
    assert_raises(Bud::CompileError) {p5.tick}    # Issue 191
  end

  def test_rename_join
    p = RenameJoin.new
    p.tick
    assert_equal([['a', 1]], p.out.to_a)
    assert_equal([['a', 1]], p.out2.to_a)
  end

  def test_partial_combos
    p = PartlyQualifiedCombo.new
    p.tick
    assert_equal(1, p.result1.length)
    assert_equal(p.result2.to_a[0].sort, p.result1.to_a[0].sort)
  end

  def test_issue_313
    i = Issue313.new
    i.explicit_tc <+ [[1, 10000]]
    i.sem_hist <+ [[10000, 1]]
    i.use_tiebreak <+ [[1, 2]]
    i.tick
    assert_equal([], i.result.to_a.sort)
  end

  class FlattenJoins
    include Bud
    state do
      table :t1, [:key_1] => [:val]
      table :t2
    end
    bootstrap do
      t1 <+ [[1,1]]
      t2 <+ [[1,2], [2,3]]
    end
    bloom do
      temp :out <= (t1 * t2).flatten
      temp :out2 <= (t1 * t2).flatten(:key_1 => :key)
    end
  end

  def test_flatten_joins
    p = FlattenJoins.new
    p.tick
    assert_equal(2, p.out.length)
    assert_equal(1, p.out2.length)
  end

  class InspectJoins
    include Bud
    state do
      table :t1
      table :t2
      table :t3
    end
    bootstrap do
      t1 << [1,1]
      t2 << [1,2]
      t3 << [1,3]
    end
    bloom do
      temp :out <= (t1 * t2).inspected
      temp :out2 <= (t1 * t2 * t3).inspected
    end
  end

  def test_inspect_joins
    p = InspectJoins.new
    p.tick
  end

  class LeftJoinChannel
    include Bud
    state do
      loopback :c
      table :t
    end
    bootstrap do
      t <+ [[2,1]]
    end
    bloom do
      temp :out <= (c * t).outer(:val => :key)
    end
  end

  def test_left_join_channel
    p = LeftJoinChannel.new
    p.run_bg
    p.sync_callback(:c, [[p.ip_port,1]], :out)
  end

  class SharedJoin
    include Bud
    state do
      table :t1
      table :t2
      table :t3
      scratch :out1, [:key1, :key2] => [:val]
      scratch :out2
    end
    bootstrap do
      t1 <+ [[1,1]]
      t1 <+ [[2,1]]
      t1 <+ [[3,2]]
      t2 <+ [[1,1]]
      t2 <+ [[2,2]]
      t2 <+ [[3,2]]
    end
    bloom do
      out1 <= (t1 * t2).pairs(:val=>:val) {|a,b| [a.key, b.key, a.val]}
      out2 <= (t1 * t2).pairs(:val=>:val) {|a,b| [a.key, a.val]}
    end
  end

  def test_shared_join
    p = SharedJoin.new
    p.tick
    assert_equal([[1, 1, 1], [2, 1, 1], [3, 2, 2], [3, 3, 2]].to_set, p.out1.to_set)
    assert_equal([[1, 1], [2, 1], [3, 2]], p.out2.to_a.sort)
  end

  class CascadedMatchJoins
    include Bud
    state do
      table :t1
      table :t2, [:key] => [:sal]
      table :t3, [:key] => [:cal]
    end
    bootstrap do
      t1 <= [[1,2]]
      t2 <= [[1,3]]
      t3 <= [[1,4]]
    end
    bloom do
      temp :outleft <= (t1 * t2).matches.lefts
      temp :outright <= (t1 * t2).matches.rights
      temp :outlpairs <= (t1 * t2).pairs(:key => :key).lefts
      temp :outrpairs <= (t1 * t2).pairs(:key => :key).rights
    end
  end

  def test_cascaded_match_joins
    p = CascadedMatchJoins.new
    p.tick
    assert_equal([[1,2]], p.outleft.to_a)
    assert_equal([[1,3]], p.outright.to_a)
    assert_equal(p.outleft.to_a, p.outlpairs.to_a)
    assert_equal(p.outright.to_a, p.outrpairs.to_a)
  end

  class TestBug179
    include Bud

    state do
      table :node, [:uid] => [:addr]
      scratch :node_cnt, [] => [:num]
      scratch :node_ready, [] => [:ready]
      table :result, [:r]
    end

    bootstrap do
      node <= [[0, "abc1"],
               [1, "abc2"],
               [2, "abc3"]]
    end

    bloom do
      node_cnt <= node.group(nil, count)
      node_ready <= node_cnt {|c| [true] if c.num == 3}

      result <= (node_ready * node).rights do |n|
        ["#1: #{n.addr}"] if n.uid == 0
      end
      result <= (node * node_ready).lefts do |n|
        ["#2: #{n.addr}"] if n.uid == 0
      end
    end
  end

  def test_bug_179
    b = TestBug179.new
    b.tick
    assert_equal([["#1: abc1"], ["#2: abc1"]], b.result.to_a.sort)
  end
end

class TestJoinLocalPreds
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4, [:c1, :c2]
  end

  bootstrap do
    t1 << [1,1]
    t1 << [2,3]
    t2 << [3,3]
    t2 << [4,5]
  end

  bloom do
    t3 <= (t1 * t2).pairs(t2.key => t2.val)
    t4 <= (t1 * t2).pairs(t1.key => t1.val)
  end
end

class TestLocalPredJoins < MiniTest::Unit::TestCase
  def test_explicit
    assert_raises(Bud::CompileError) {p = TestJoinLocalPreds.new; p.tick}
    # assert_equal([ [[1,1], [3,3]], [[2,3], [3,3]] ], p.t3.to_a.sort)
    # assert_equal([ [[1,1], [3,3]], [[1,1], [4,5]] ], p.t4.to_a.sort)
  end
end

class Issue192
 include Bud

 state do
   table :intab1, [:ycol, :x]
   table :intab2, [:x]
   table :outtab1, [] => [:x]
   table :outtab2, [] => [:x]
 end

 bloom do
   outtab1 <= (intab1 * intab2).rights {|k| [k.x + 1]}
   outtab2 <= (intab1 * intab2).pairs {|j, k| [k.x + 1]}
 end
end

class TestIssue192 < MiniTest::Unit::TestCase
  def test_192
    p = Issue192.new
    p.intab1 << [-1]
    p.intab2 << [-1]
    p.tick
    assert_equal([[0]], p.outtab1.to_a)
    assert_equal([[0]], p.outtab2.to_a)
  end
end

class TestIssue220 < MiniTest::Unit::TestCase
  class TripleJoin
    include Bud
    state do
      scratch :foo
    end
    bloom do
      stdio <~ (foo*foo*foo).combos(:key=>:val){|f1, f2, f3| [f1.val,f2.val,f3.val]}
      foo <= [[1,0]]
    end
  end
  def test_triple_join
    assert_raises(Bud::CompileError){p = TripleJoin.new; p.tick}
  end
end

class OjChannel
  include Bud

  state do
    table :user_db, [:user] => [:password]
    channel :req, [:@addr, :client, :user] => [:password]
    channel :resp, [:@addr, :user] => [:password]
  end

  bootstrap do
    user_db <= [["nrc", "qwerty"], ["jmh", "password"]]
  end

  bloom do
    resp <~ (req * user_db).outer(:user => :user) do |r, u|
      [r.client, r.user, r.password == u.password]
    end
  end
end

class OjMultiplePreds
  include Bud

  state do
    table :t1
    table :t2
    scratch :t3
  end

  bloom do
    t3 <= (t1 * t2).outer(:key => :key, :val => :val) {|x,y| x}
  end
end

class OjMultipleRelsError
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :s4
  end

  bloom do
    s4 <= (t1 * t2 * t3).outer(:key => :key)
  end
end

class TestOuterJoins < MiniTest::Unit::TestCase
  def test_oj_channel
    o = OjChannel.new
    o.run_bg
    rv = o.sync_callback(:req, [[o.ip_port, o.ip_port, "nrc", "qwerty"]], :resp)
    assert_equal([[o.ip_port, "nrc", true]], rv.to_a.sort)
    rv = o.sync_callback(:req, [[o.ip_port, o.ip_port, "jmh", "qwerty"]], :resp)
    assert_equal([[o.ip_port, "jmh", false]], rv.to_a.sort)
    rv = o.sync_callback(:req, [[o.ip_port, o.ip_port, "franklin", "cs186"]], :resp)
    assert_equal([[o.ip_port, "franklin", false]], rv.to_a.sort)
    o.sync_do {
      o.user_db <+ [["franklin", "cs186"]]
    }
    rv = o.sync_callback(:req, [[o.ip_port, o.ip_port, "franklin", "cs186"]], :resp)
    assert_equal([[o.ip_port, "franklin", true]], rv.to_a.sort)
    o.stop
  end

  def test_oj_multi_preds
    i = OjMultiplePreds.new
    i.t1 <+ [[5, 10]]
    i.t2 <+ [[5, 11]]
    i.tick

    assert_equal([[5, 10]], i.t3.to_a.sort)
  end

  def test_oj_multi_rel_error
    i = OjMultipleRelsError.new
    assert_raises(Bud::Error) { i.tick }
  end
end

class TestScanReplay
  include Bud

  state do
    table :t1
    scratch :x1
    scratch :x2
  end

  bloom do
    x2 <= t1
    x2 <= x1
  end
end

class TestJoinReplay
  include Bud

  state do
    table :t1
    table :t2
    scratch :x1
    scratch :x2
    scratch :x3
  end

  bloom do
    x1 <= (t1 * t2).pairs {|x,y| [x.key, y.val]}
    x1 <= (x2 * x3).pairs {|x,y| [x.key, y.val]}
  end
end

class TestAggReplay
  include Bud

  state do
    table :t1
    scratch :x1
    scratch :x2
  end

  bloom do
    x1 <= t1.group([:key], max(:val))
    x1 <= x2.group([:key], max(:val))
  end
end

class TestReduceReplay
  include Bud

  state do
    table :t1, [:v, :x]
    scratch :x1, [:res]
    scratch :x2, [:v, :x]
  end

  bloom do
    x1 <= t1.reduce([[true]]) do |memo, t|
      if t.v == false
        [[false]]
      else
        memo
      end
    end
    x1 <= x2.reduce([[true]]) do |memo, t|
      if t.v == false
        [[false]]
      else
        memo
      end
    end
  end
end

class TestOuterJoinReplay
  include Bud

  state do
    table :t1
    table :t2
    scratch :s1
  end

  bloom do
    s1 <= (t1 * t2).outer(:key => :key) {|x, y|
      y == [nil,nil] ? [x.key + 1, x.val + 1] : [x.key, x.val]
    }
  end
end

# Issue #276 and related bugs
class RescanTests < MiniTest::Unit::TestCase
  def test_scan_replay
    i = TestScanReplay.new
    i.t1 <+ [[4, 8]]
    i.x1 <+ [[5, 10]]
    i.tick
    assert_equal([[4, 8], [5, 10]], i.x2.to_a.sort)
    i.tick
    assert_equal([[4, 8]], i.x2.to_a.sort)
    i.x1 <+ [[5, 10]]
    i.tick
    assert_equal([[4, 8], [5, 10]], i.x2.to_a.sort)
  end

  def test_join_replay
    i = TestJoinReplay.new
    i.t1 <+ [[2, 3]]
    i.t2 <+ [[3, 4]]
    i.x2 <+ [[5, 6]]
    i.x3 <+ [[7, 8]]
    i.tick
    assert_equal([[2, 4], [5, 8]], i.x1.to_a.sort)
    i.x2 <+ [[5, 6]]
    i.x3 <+ [[7, 8]]
    i.tick
    assert_equal([[2, 4], [5, 8]], i.x1.to_a.sort)
  end

  def test_agg_replay
    i = TestAggReplay.new
    i.t1 <+ [[5, 10], [6, 11]]
    i.x2 <+ [[7, 12]]
    i.tick
    assert_equal([[5, 10], [6, 11], [7, 12]], i.x1.to_a.sort)
    i.x2 <+ [[7, 13]]
    i.tick
    assert_equal([[5, 10], [6, 11], [7, 13]], i.x1.to_a.sort)
  end

  def test_reduce_replay
    i = TestReduceReplay.new
    i.t1 <+ [[true, 1], [true, 2]]
    i.x2 <+ [[true, 1], [false, 2]]
    i.tick
    assert_equal([[true], [false]], i.x1.to_a.sort {|a,b| a == true ? 1 : 0})
    i.tick
    assert_equal([[true]], i.x1.to_a.sort {|a,b| a == true ? 1 : 0})
    i.x2 <+ [[true, 1], [false, 2]]
    i.tick
    assert_equal([[true], [false]], i.x1.to_a.sort {|a,b| a == true ? 1 : 0})
  end

  def test_oj_replay
    i = TestOuterJoinReplay.new
    i.t1 <+ [[5, 10]]
    i.tick
    i.t2 <+ [[5, 11]]
    i.tick
    assert_equal([[5, 10]], i.s1.to_a)
  end
end
