require './test_common'

class NotInTest < MiniTest::Unit::TestCase
  class SillyAnti
    include Bud

    state do
      table :emp
      scratch :mgrs, [:name]
      scratch :outsie
      scratch :sillyblock_out
      scratch :realblock_out
      scratch :emptiness
    end
    bootstrap {emp <= [['bob', 1], ['betsy', 1], ['caitlin', 0]]}
    bloom do
      mgrs <= emp {|e| [e.key] if e.val > 0}
      outsie <= emp.notin(mgrs, :key => :name)
      realblock_out <= emp.notin(mgrs) do |e,m|
         e.key == m.name
      end
      sillyblock_out <= emp.notin(mgrs, :key => :name) do |e,m|
        e.key == 'bob'
      end
    end
  end

  def test_silly_anti
    o = SillyAnti.new
    strat = [["emp", "mgrs"], ["outsie", "realblock_out", "sillyblock_out"]]
    strat.each_with_index do |vals, i|
      vals.each {|v| assert_equal(i, o.collection_stratum(v))}
    end
    o.tick
    assert_equal([['betsy'], ['bob']], o.mgrs.to_a.sort)
    assert_equal([['caitlin', 0]], o.outsie.to_a)
    assert_equal([['caitlin', 0]], o.realblock_out.to_a)
    assert_equal([['betsy', 1], ['caitlin', 0]], o.sillyblock_out.to_a.sort)
  end
end

class NotInTest2 < MiniTest::Unit::TestCase
  class SimpleNotIn
    include Bud
    state do
      table :foo, [:c1, :c2]
      table :bar, [:c1, :c2]
      table :outsie, [:c1, :c2]
    end
    bootstrap do
      foo <= [["alex", 1], ["joe", 2], ["jonathan", 3]]
      bar <= [["joe", 0], ["joe", 1], ["alex", 1]]
    end
    bloom do
      outsie <= foo.notin(bar, :c1=>:c1) {|f, b| f.c2 <= b.c2 }
    end
  end

  def test_simple_notin
    o = SimpleNotIn.new
    strat = [["foo", "bar"], ["outsie"]]
    strat.each_with_index do |vals, i|
      vals.each {|v| assert_equal(i, o.collection_stratum(v))}
    end
    o.tick
    assert_equal([["joe", 2], ["jonathan", 3]], o.outsie.to_a.sort)
  end
end

class BlocklessNotInTest < MiniTest::Unit::TestCase
  class BlocklessNotIn
    include Bud
    state do
      table :foo, [:c1, :c2]
      table :bar, [:c3, :c4]
      table :outsie, [:c1, :c2]
      table :outsie2, [:c1, :c2]
      table :outsie3, [:c1, :c2]
      table :outsie4, [:c1, :c2]
    end
    bootstrap do
      foo <= [["alex", 1], ["jonathan", 2], ["jonathan", 3]]
      bar <= [["jonathan", 2], ["alex", 1]]
    end
    bloom do
      outsie <= foo.notin(bar)
      outsie2 <= foo.notin(bar, :c1=>:c3)
      outsie3 <= foo.notin(bar, :c2=>:c4)
    end
  end
  def test_blockless_notin
    o = BlocklessNotIn.new
    o.tick
    assert_equal([["jonathan", 3]], o.outsie.to_a)
    assert_equal([], o.outsie2.to_a)
    assert_equal([["jonathan", 3]], o.outsie3.to_a)
  end
end

class RecursiveNotInTest < MiniTest::Unit::TestCase # issue 255
  class RecNotIn
    include Bud
    state do
      table :link, [:from, :to]
      table :path, link.schema
      table :path_buf, link.schema
      table :avoid,  link.schema
    end
    bootstrap do
      link <= [['a', 'b'], ['b', 'c'], ['c', 'd']]
      avoid <= [['a', 'b']]
    end
    bloom do
      path_buf <= link
      path_buf <= (path * link).pairs {|p, l| [p.from, l.to]}
      path <= path_buf.notin(avoid)
      path <= path_buf
    end
  end
  def test_rec_notin
    o = RecNotIn.new
    o.tick
  end
end


class StratifiedTest < MiniTest::Unit::TestCase # issue 271
  class StratNotIn
    include Bud
    state do
      table :link, [:from, :to]
      table :path, link.schema
      table :final_path, link.schema
      scratch :fp, link.schema
      table :excludes, link.schema
    end
    bloom do
      path <= link
      path <= (link * path).pairs(:to => :from) {|l,p| [l.from, p.to]}
      fp <= path.notin(excludes, :from => :from)  # indirection to final_path via fp (issue 271)
      final_path <= fp
    end
  end
  class Unstrat < StratNotIn
    bloom do
      path <= link.notin(path) # Meaningless snippet, and should be marked unstratifiable.
    end
  end
  def test_strat
    o = StratNotIn.new
    o.link <+ [[1,2], [2,3], [1,3], [3,4]]
    o.excludes <+ [[1, :dummy]] # excludes all paths starting from 1. The :to field is never seen.
    o.tick
    assert_equal([[2,3],[2,4],[3,4]], o.final_path.to_a.sort)
  end

  def test_unstrat
    assert_raises(Bud::CompileError) {Unstrat.new}
  end
end

class RescanNotIn
  include Bud

  state do
    table :in_t
    table :not_in
    scratch :res_t
  end

  bloom do
    res_t <= in_t.notin(not_in)
  end
end

class RescanNotInScratch
  include Bud

  state do
    scratch :in_t
    scratch :not_in
    scratch :res_t
  end

  bloom do
    res_t <= in_t.notin(not_in)
  end
end

class RescanNotInMix
  include Bud

  state do
    scratch :inp, [:payload]
    table :delivered, inp.schema
    scratch :outp, inp.schema
  end

  bloom do
    outp <= inp.notin(delivered, :payload => :payload)
    delivered <+ inp
  end
end

class RescanNotInTest < MiniTest::Unit::TestCase
  def test_notin_rescan
    i = RescanNotIn.new
    i.in_t <+ [[5, 10]]
    i.tick
    assert_equal([[5, 10]], i.res_t.to_a)
    i.tick
    assert_equal([[5, 10]], i.res_t.to_a)
    i.not_in <+ [[5, 10]]
    i.tick
    assert_equal([], i.res_t.to_a)
  end

  def test_notin_rescan_scratch
    i = RescanNotInScratch.new
    i.in_t <+ [[5, 10]]
    i.tick
    assert_equal([[5, 10]], i.res_t.to_a)
    i.tick
    assert_equal([], i.res_t.to_a)
    i.in_t <+ [[10, 20], [30, 40]]
    i.not_in <+ [[10, 20]]
    i.tick
    assert_equal([[30, 40]], i.res_t.to_a)
  end

  def test_notin_rescan_mix
    i = RescanNotInMix.new
    i.inp <+ [["foo"]]
    i.tick
    assert_equal([["foo"]], i.outp.to_a)
    3.times do
      i.inp <+ [["foo"]]
      i.tick
      assert_equal([], i.outp.to_a)
    end

    i.inp <+ [["bar"], ["foo"]]
    i.tick
    assert_equal([["bar"]], i.outp.to_a)
    3.times do
      i.inp <+ [["bar"], ["foo"]]
      i.tick
      assert_equal([], i.outp.to_a)
    end
  end
end

class TwoQuals
  include Bud

  state do
    table :lside
    table :rside
    table :result
  end

  bloom do
    result <= rside.notin(lside, :key => :key, :val => :val)
  end
end

class TwoQualsChain
  include Bud

  state do
    table :lside
    table :rside, [:key, :val]
    table :result
  end

  bloom do
    result <= rside.argmax([:key], :val).notin(lside, :key => :key, :val => :val)
  end
end

class TwoQualsTest < MiniTest::Unit::TestCase
  # issue 282
  def test_two_quals
    f = TwoQuals.new
    f.lside <+ [[1, 2]]
    f.rside <+ [[1, 3]]
    f.tick
    assert_equal([[1, 3]], f.result.to_a)
  end

  def test_two_quals_chain
    f = TwoQualsChain.new
    f.lside <+ [[1, 2]]
    f.rside <+ [[1, 3], [1, 1], [1, 2]]
    f.tick
    assert_equal([[1, 3]], f.result.to_a)
  end
end

class NotInSelf
  include Bud

  state do
    table :t1, [:ident] => [:foo]
    scratch :res, t1.schema
  end

  bloom do
    res <= t1.notin(t1, :foo => :ident)
  end
end

class NotinSelfChainStackBug
  include Bud

  state do
    scratch :r0_t1_t1_in_buf, [:lhs_x, :lhs_y, :rhs_x, :rhs_y]
    scratch :r0_t1_t1_match_buf, [:lhs_x, :lhs_y, :rhs_x, :rhs_y]
    scratch :r0_t1_t1_missing, [:lhs_x, :lhs_y, :rhs_x, :rhs_y]
    scratch :r1, [:a, :b, :c, :d]
    table :seal_t1, [:ignored]
    table :t1, [:x, :y]
    table :t2, r1.schema
  end

  bloom do
    r1 <= ((t1 * t1).pairs(:x => :y) { |a, b| (a + b) }.notin(t2))
    r0_t1_t1_in_buf <= (t1 * t1).pairs(t1.x => t1.y) {|x,y| x + y}
    r0_t1_t1_match_buf <= (r0_t1_t1_in_buf * t2).lefts(:lhs_x => :a, :lhs_y => :b, :rhs_x => :c, :rhs_y => :d)
    r0_t1_t1_missing <= (t1 * t1).pairs(t1.x => t1.y) {|x,y| x + y}.notin(r0_t1_t1_match_buf)
    t1 <- (t1 * seal_t1).lefts.notin(r0_t1_t1_missing, :x => :lhs_x, :y => :lhs_y).notin(r0_t1_t1_missing, :x => :rhs_x, :y => :rhs_y)
  end
end

class NotInSelfTest < MiniTest::Unit::TestCase
  def test_self_notin
    n = NotInSelf.new
    n.t1 <+ [[5, 10], [10, 10], [12, 11]]
    n.tick
    assert_equal([[12, 11]], n.res.to_a.sort)

    n.t1 <+ [[11, 13]]
    n.tick
    assert_equal([[11, 13]], n.res.to_a.sort)

    n.t1 <+ [[13, 10]]
    n.tick
    assert_equal([], n.res.to_a.sort)
  end

  def test_notin_join_stack
    r = NotinSelfChainStackBug.new
    r.t1 <+ [[1, 2], [2, 3], [3, 4]]
    r.t2 <+ [[1, 2, 3, 4], [2, 3, 1, 2]]
    2.times { r.tick }

    assert_equal([[3, 4, 2, 3]].to_set, r.r1.to_set)
    assert_equal([[1, 2], [2, 3], [3, 4]].to_set, r.t1.to_set)

    r.seal_t1 <+ [[true]]
    2.times { r.tick }

    assert_equal([[3, 4, 2, 3]].to_set, r.r1.to_set)
    assert_equal([[2, 3], [3, 4]].to_set, r.t1.to_set)
  end
end

class NotInChain
  include Bud

  state do
    scratch :t1
    scratch :t2_implicit
    table :t2, [:foo2, :bar2]
    table :t3, [:foo3, :bar3]
    table :t4, [:foo4, :bar4]
    scratch :t5
  end

  bloom do
    t1 <= t2.notin(t3, :foo2 => :bar3).notin(t4, :foo2 => :foo4)
    t2_implicit <= t2
    t5 <= t2_implicit.notin(t3, :key => :bar3).notin(t4, :key => :foo4)
  end
end

class NotInChainJoin
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :res1
    table :res2
  end

  bloom do
    res1 <= (t1 * t2).lefts(:val => :key).notin(t3)
    res2 <= ((t1 * t3).pairs(:val => :key) {|x,y| [x.key + 1, x.val + 1]}).notin(t3)
  end
end

class NotInChainTest < MiniTest::Unit::TestCase
  def test_chain_notin
    n = NotInChain.new
    n.t2 <+ [[4, 9], [5, 10], [6, 11], [8, 10], [12, 9]]
    n.t3 <+ [[20, 5], [21, 6], [22, 7]]
    n.t4 <+ [[8, 8], [9, 9]]
    n.tick
    assert_equal([[4, 9], [12, 9]], n.t1.to_a.sort)
    assert_equal([[4, 9], [12, 9]], n.t5.to_a.sort)

    n.t3 <+ [[5, 12]]
    n.tick
    assert_equal([[4, 9]], n.t1.to_a.sort)
    assert_equal([[4, 9]], n.t5.to_a.sort)
  end

  def test_chain_join
    n = NotInChainJoin.new
    n.t1 <+ [[1, 2], [3, 4], [5, 6]]
    n.t2 <+ [[2, 4], [6, 8]]
    n.t3 <+ [[1, 2], [2, 4], [6, 7]]
    n.tick
    assert_equal([[5, 6]], n.res1.to_a.sort)
    assert_equal([[2, 3]], n.res2.to_a.sort)
  end
end

class StratError
  include Bud

  state do
    table :resp_log, [:addr, :id, :key] => [:val]
    table :req_log, resp_log.schema
    table :did_resp, resp_log.schema
    scratch :need_resp, resp_log.schema
  end

  bloom do
    resp_log <= need_resp
    need_resp <= req_log.notin(did_resp)
    did_resp <= resp_log
  end
end

class StratShouldWork
  include Bud

  state do
    scratch :r2
    table :t4
    scratch :t5
    table :t20
  end

  bloom do
    r2 <= t4.notin(t5)
    t5 <= (t5 * t20).lefts
  end
end

class TestStratErrors < MiniTest::Unit::TestCase
  def test_strat_error
    assert_raises(Bud::CompileError) { StratError.new }
  end

  def test_strat_precycleneg
    StratShouldWork.new
    assert(true)
  end
end
