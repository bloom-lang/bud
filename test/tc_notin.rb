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

class TwoQualsTest < MiniTest::Unit::TestCase
  # issue 282
  def test_twoquals
    f = TwoQuals.new
    f.lside <+ [[1, 2]]
    f.rside <+ [[1, 3]]
    f.tick
    assert_equal([[1, 3]], f.result.to_a)
  end
end
