require 'test_common'

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
    t4 <= (r1 * r2 * r3).pairs(:k4 => :k6) {|r,s,t| r+s+t}
    t5 <= (t1 * t2 * t3).combos(t1.key => t3.key) {|r,s,t| r+s+t}
    t6 <= (r3 * r4 * r5).matches {|a,b,c| a+b+c}
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
    temp :j2 <= (r * s_tab).pairs(:x => :x)
    simple_out <= j2 {|t1,t2| [t1.x, t1.y1, t2.y1] }

    temp :k <= (r * s_tab).pairs(:x => :x, :y1 => :y1)
    match_out <= k { |t1,t2| [t1.x, t1.y1, t2.y1] }

    temp :l <= (r * s_tab * t).combos(r.x => s_tab.x, s_tab.x => t.x)
    chain_out <= l { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y1, t2.y1, t3.y1] }

    temp :n <= (r * s_tab * t).matches
    nat_out <= n.map { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y1, t2.y1, t3.y1] }

    temp :newtab <= (r * s_tab * t).combos(r.x => s_tab.x, s_tab.x => t.x)
    temp :newtab_out <= newtab { |a,b,c| [a.x, b.x, c.x, a.y1, b.y1, c.y1] }

    temp :loj <= (mismatches * s_tab).outer(:x => :x)
    loj_out <= loj.map { |t1, t2| [t1.x, t2.x, t1.y1, t2.y1] }
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
  end
end

class PartlyQualifiedCombo
  include Bud
  state do
    table :arr
    table :ess
    table :tee
    table :result1
    table :result2
  end

  bootstrap do
    arr << [1, 2]
    ess << [1, 3]
    tee << [5, 6]
  end

  bloom do
    # result is never populated
    result1 <= (tee * arr * ess).combos(arr.key => ess.key)
    # but it is when the join is specified in this order
    result2 <= (arr * ess * tee).combos(arr.key => ess.key)
  end
end


class TestJoins < Test::Unit::TestCase
  def test_combos
    program = CombosBud.new
    program.tick
    simple_outs = program.simple_out
    assert_equal(7, simple_outs.length)
    assert_equal(1, simple_outs.select { |t| t[0] == 'a'} .length)
    assert_equal(2, simple_outs.select { |t| t[0] == 'b'} .length)
    assert_equal(4, simple_outs.select { |t| t[0] == 'c'} .length)
  end

  def test_secondary_join_predicates
    program = CombosBud.new
    program.tick
    match_outs = program.match_out
    assert_equal(4, match_outs.length)
    assert_equal(1, match_outs.select { |t| t[0] == 'a'} .length)
    assert_equal(1, match_outs.select { |t| t[0] == 'b'} .length)
    assert_equal(2, match_outs.select { |t| t[0] == 'c'} .length)
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
    assert_equal([1,2,3,4,5], program.num.to_a.sort.flatten)
  end

  def test_block_append
    program = BlockAppend.new
    program.tick
    assert_equal([1,2,3,4,5], program.num.to_a.sort.flatten)
  end

  def test_left_outer_join
    program = CombosBud.new
    program.tick
    loj_outs = program.loj_out
    assert_equal(3, loj_outs.length)
    assert_equal(loj_outs.to_a.sort, [["a", "a", 1, 1], ["v", nil, 1, nil], ["z", nil, 1, nil]])
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
    p2 = MissingAttrRefs.new
    p3 = IllegalAttrRefs.new
    p4 = AmbiguousAttrRefs.new
    p5 = UnJoinedTableRef.new    # Issue 191
    assert_raise(Bud::CompileError) {p1.tick}
    assert_raise(Bud::CompileError) {p2.tick}
    assert_raise(Bud::CompileError) {p3.tick}
    assert_raise(Bud::CompileError) {p4.tick}
    assert_raise(Bud::CompileError) {p5.tick}
  end
  
  def test_rename_join
    p = RenameJoin.new
    p.tick
    assert_equal([['a', 1]], p.out.to_a)
  end

  def test_partial_combos
    p = PartlyQualifiedCombo.new
    p.tick
    assert_equal(1, p.result1.length)
    assert_equal(p.result2.to_a.flatten.sort, p.result1.to_a.flatten.sort)
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
    p.tick; p.tick
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
      temp :out1 <= (t1 * t2).pairs(:val=>:val) {|a,b| [a.key, b.key, a.val]}
      temp :out2 <= (t1 * t2).pairs(:val=>:val) {|a,b| [a.key, a.val]}
    end
  end
  
  def test_shared_join
    p = SharedJoin.new
    p.tick; p.tick
    assert_equal([[1, 1, 1], [2, 1, 1], [3, 2, 2], [3, 3, 2]], p.out1.to_a.sort)
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
    p.tick; p.tick
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

class TestLocalPredJoins < Test::Unit::TestCase
  def test_explicit
    assert_raise(Bud::CompileError) {p = TestJoinLocalPreds.new; p.tick}
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

class TestIssue192 < Test::Unit::TestCase
  def test_192
    p = Issue192.new
    p.intab1 << [-1]
    p.intab2 << [-1]
    p.tick;
    assert_equal([[0]], p.outtab1.to_a)
    assert_equal([[0]], p.outtab2.to_a)    
  end
end


class TestIssue220 < Test::Unit::TestCase
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
    assert_raise(Bud::CompileError){p = TripleJoin.new; p.tick}
  end
end