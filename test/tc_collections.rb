require 'test_common'

class BabyBud
  include Bud

  state {
    scratch :scrtch, [:k1, :k2] => [:v1, :v2]
    scratch :scrtch2, [:k1, :k2]
    table :tbl, [:k1, :k2] => [:v1, :v2]
  }

  bootstrap do
    scrtch <= [['a', 'b', 1, 2]]
    scrtch <= [['a', 'c', 3, 4]]
    scrtch2 <= [['a', 'b']]
    tbl <= [['a', 'b', 1, 2]]
    tbl <= [['z', 'y', 9, 8]]
  end

  declare
  def rules
    scrtch <+ [['c', 'd', 5, 6]]
    tbl <+ [['c', 'd', 5, 6]]
    tbl <- [['a', 'b', 1, 2]]
  end
end

class DupKeyBud
  include Bud

  state {
    scratch :tab, [:k] => [:name]
  }

  declare
  def program
    tab <= [[2000, 'bush']]
    tab <= [[2000, 'gore']]
  end
end

class DupTableBud
  include Bud

  state {
    scratch :s, [:k]
    scratch :s, [:l]
  }
end

class DupColBud
  include Bud

  state {
    scratch :silly, [:a, :a]
  }
end

class Grep
  include Bud
  attr_reader :pattern

  def initialize(pattern)
    super()
    @pattern = pattern
  end

  state {
    file_reader :text, 'text/ulysses.txt'
    table :matches, [:lineno, :text]
  }

  declare
  def program
    matches <= text.map{|t| t if t.text =~ pattern}
  end
end

class Union
  include Bud

  state {
    table :link, [:from, :to, :cost]
    table :delta_link, [:from, :to, :cost]
    table :union, [:from, :to, :cost]
  }

  bootstrap do
    link <= [['a', 'b', 1]]
    delta_link <= [['a', 'b', 4]]
  end

  declare
  def prog
    union <= (delta_link <= link)
  end
end

class DeleteKey
  include Bud

  state {
    table :t1, [:k] => [:v]
    table :del_buf, [:k, :v]
  }

  bootstrap do
    t1 << [5, 10]
  end

  declare
  def rules
    t1 <- del_buf
  end
end

class BootstrapDerive
  include Bud

  state do
    table :t1
    scratch :t2
    scratch :t3
  end

  bootstrap do
    t1 << [5,10]
  end

  declare
  def rules
    t2 <= t1.map{|t| [t.key + 1, t.val + 1]}
    t3 <= t2.map{|t| [t.key + 1, t.val + 1]}
  end
end

class RowValueTest
  include Bud

  state {
    table :t1, [:k] => [:v]
    table :t2, [:k] => [:v]
    table :t3, [:k] => [:v]
    table :t4, [:k] => [:v]
  }

  declare
  def rules
    t3 <= t1.map {|t| t if t2.include? t}
    t4 <= t1.map {|t| t if t2.has_key? [t.k]}
  end
end

class BendTypes
  include Bud

  state do
    table :t1
  end

  bootstrap do
    t1 <= {1=>'a', 2=>'b'}
  end
end

class BendTypesDelete
  include Bud

  state do
    table :t1, [:k1, :k2]
    table :t2, [:k1, :k2]
  end

  bootstrap do
    t1 << [5, nil]
    t1 << [5, 10]
  end

  declare
  def rules
    t1 <- t2.map {|t| [t.k1]}
  end
end

class NonEnumerable
  include Bud

  state {
    table :t1
  }

  declare
  def rules
    t1 <= true
  end
end

class NonTuple
  include Bud

  state {
    table :t1
  }

  declare
  def rules
    t1 <= [1,2,3]
  end
end

class NonTupleDelete
  include Bud

  state do
    table :t1
  end

  declare
  def rules
    t1 <- [1,2]
  end
end

class DupTableDef
  include Bud

  state do
    table :t1
    scratch :t1
  end
end

class DelBug
  include Bud

  state do
    scratch :start
    table :buffer
    periodic :tic, 1
  end

  declare
  def logos
    buffer <= start
    buffer <- join([tic, buffer]) do |t, h|
      h if h.key == 'foo'
    end
  end
end

class TestCollections < Test::Unit::TestCase
  def test_simple_deduction
    program = BabyBud.new
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(2, program.scrtch.length)
    assert_equal(1, program.scrtch2.length)
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(0, program.scrtch2.length)
    assert_equal(2, program.tbl.length)
  end

  def test_tuple_accessors
    program = BabyBud.new
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(1, program.scrtch[['a','b']].v1)
    assert_equal(2, program.scrtch[['a','b']].v2)
  end

  def test_insert_delete
    program = BabyBud.new
    # tick twice to get to 2nd timestep
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(1, program.scrtch.length )
    assert_equal(0, program.scrtch2.length )
    assert_equal(2, program.tbl.length )
  end

  def test_dup_tables
    assert_raise(Bud::BudError) {program = DupTableBud.new}
  end

  def test_dup_columns
    assert_raise(Bud::BudError) {program = DupColBud.new}
  end

  def test_dup_keys
    program = DupKeyBud.new
    assert_raise(Bud::KeyConstraintError) { program.tick }
  end

  def test_grep
    program = Grep.new(/[Bb]loom/)
    assert_nothing_raised(RuntimeError) { program.tick }
    lines = program.matches.map{|t| t}
    assert_equal(1, lines.length)
    assert_equal(44, lines[0][0])
  end

  def test_union
    s = Union.new
    assert_nothing_raised(RuntimeError) { s.tick }
    assert_equal(2, s.union.length)
    assert_equal([["a", "b", 4], ["a", "b", 1]], s.union.to_a)
  end

  def test_delete_key
    d = DeleteKey.new
    assert_nothing_raised(RuntimeError) { d.tick }
    assert_equal(1, d.t1.length)
    d.del_buf << [5, 11] # shouldn't delete
    assert_nothing_raised(RuntimeError) { d.tick }
    assert_equal(1, d.t1.length)
    assert_nothing_raised(RuntimeError) { d.tick }
    assert_equal(1, d.t1.length)

    d.del_buf << [5, 10] # should delete
    assert_nothing_raised(RuntimeError) { d.tick }
    assert_equal(1, d.t1.length)
    assert_nothing_raised(RuntimeError) { d.tick }
    assert_equal(0, d.t1.length)
  end

  def test_row_equality
    rv = RowValueTest.new
    rv.run_bg
    rv.sync_do {
      rv.t1 << [5, 10]
      rv.t1 << [6, 11]
      rv.t2 << [5, 10]
      rv.t2 << [7, 12]
      rv.t2 << [6, 15]
    }

    rv.sync_do {
      assert_equal(1, rv.t3.length)
      assert_equal(2, rv.t4.length)
    }

    rv.stop_bg
  end

  def test_types
    p1 = BendTypes.new
    assert_nothing_raised(RuntimeError) { p1.tick }
    assert_equal(1, p1.t1.first.key)
    p2 = NonEnumerable.new
    assert_raise(Bud::BudTypeError) { p2.tick }
    p3 = NonTuple.new
    assert_raise(Bud::BudTypeError) { p3.tick }
    p4 = NonTupleDelete.new
    assert_raise(Bud::BudTypeError) { p4.tick }
  end

  def test_types_delete
    p = BendTypesDelete.new
    p.run_bg
    p.sync_do {
      assert_equal(2, p.t1.length)
      p.t2 <+ [[5, 100]]
    }
    p.sync_do
    p.sync_do {
      assert_equal([[5, 10]], p.t1.to_a.sort)
    }
    p.stop_bg
  end

  def test_bootstrap_derive
    b = BootstrapDerive.new
    b.run_bg
    2.times do
      b.sync_do {
        assert_equal([[6, 11]], b.t2.to_a.sort)
        assert_equal([[7, 12]], b.t3.to_a.sort)
      }
    end

    b.stop_bg
  end

  def test_dup_table_def
    assert_raise(Bud::BudError) { DupTableDef.new }
  end

  def test_filter_and_delete
    th = DelBug.new(:port => 12345)
    th.run_bg
    assert_nothing_raised do
      th.sync_do {th.start <+ [['foo','bar'], ['baz','bam']]}
      sleep 2
    end
  end
end
