require 'test_common'

class BabyBud
  include Bud

  state do
    scratch :scrtch, [:k1, :k2] => [:v1, :v2]
    scratch :scrtch2, [:k1, :k2]
    table :tbl, [:k1, :k2] => [:v1, :v2]
  end

  bootstrap do
    scrtch <= [['a', 'b', 1, 2]]
    scrtch <= [['a', 'c', 3, 4]]
    scrtch2 <= [['a', 'b']]
    tbl <= [['a', 'b', 1, 2]]
    tbl <= [['z', 'y', 9, 8]]
  end

  bloom do
    scrtch <+ [['c', 'd', 5, 6]]
    tbl <+ [['c', 'd', 5, 6]]
    tbl <- [['a', 'b', 1, 2]]
  end
end

class DupKeyBud
  include Bud

  state do
    scratch :tab, [:k] => [:name]
  end

  bloom do
    tab <= [[2000, 'bush']]
    tab <= [[2000, 'gore']]
  end
end

class DupTableBud
  include Bud

  state do
    scratch :s, [:k]
    scratch :s, [:l]
  end
end

class DupColBud
  include Bud

  state do
    scratch :silly, [:a, :a]
  end
end

class Grep
  include Bud
  attr_reader :pattern

  def initialize(pattern)
    super()
    @pattern = pattern
  end

  state do
    file_reader :text, 'text/ulysses.txt'
    table :matches, [:lineno, :text]
  end

  bloom do
    matches <= text.map{|t| t if t.text =~ pattern}
  end
end

class Union
  include Bud

  state do
    table :link, [:from, :to, :cost]
    table :delta_link, [:from, :to, :cost]
    table :union, [:from, :to, :cost]
  end

  bootstrap do
    link <= [['a', 'b', 1]]
    delta_link <= [['a', 'b', 4]]
  end

  bloom do
    union <= (delta_link <= link)
  end
end

class DeleteKey
  include Bud

  state do
    table :t1, [:k] => [:v]
    table :del_buf, [:k, :v]
  end

  bootstrap do
    t1 << [5, 10]
  end

  bloom do
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

  bloom do
    t2 <= t1.map{|t| [t.key + 1, t.val + 1]}
    t3 <= t2.map{|t| [t.key + 1, t.val + 1]}
  end
end

class RowValueTest
  include Bud

  state do
    table :t1, [:k] => [:v]
    table :t2, [:k] => [:v]
    table :t3, [:k] => [:v]
    table :t4, [:k] => [:v]
  end

  bloom do
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

  bloom do
    t1 <- t2.map {|t| [t.k1]}
  end
end

class NonEnumerable
  include Bud

  state do
    table :t1
  end

  bloom do
    t1 <= true
  end
end

class NonTuple
  include Bud

  state do
    table :t1
  end

  bloom do
    t1 <= [1,2,3]
  end
end

class NonTupleDelete
  include Bud

  state do
    table :t1
  end

  bloom do
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
    table :buffer
    table :to_delete
  end

  bootstrap do
    buffer <= [[1,2], [3,4]]
    to_delete <= [[3,4], [5,6]]
  end

  bloom do
    buffer <- to_delete.map {|t| t if t.val != 4}
  end
end

class BadDeclaration
  include Bud
  state do
    scratch 0
  end
end

class DeclarationOverrideMethod
  include Bud
  state do
    scratch :id
  end
end

class EmptyPk
  include Bud

  state do
    table :t1, [] => [:foo, :bar]
  end
end

class TestCollections < Test::Unit::TestCase
  def test_simple_deduction
    program = BabyBud.new
    program.tick
    assert_equal(1, program.scrtch2.length)
    program.tick
    assert_equal([["[\"c\", \"d\", 5, 6]"]], program.scrtch.inspected)
    assert_equal(0, program.scrtch2.length)
    assert_equal(2, program.tbl.length)
    assert_equal([["c", "d"], ["z", "y"]].sort, program.tbl.keys.sort)
    assert_equal([[5,6], [9,8]].sort, program.tbl.values.sort)
  end

  def test_tuple_accessors
    program = BabyBud.new
    program.tick
    assert_equal(1, program.scrtch[['a','b']].v1)
    assert_equal(2, program.scrtch[['a','b']].v2)
  end

  def test_insert_delete
    program = BabyBud.new
    # tick twice to get to 2nd timestep
    program.tick
    program.tick
    assert_equal(1, program.scrtch.length )
    assert_equal(0, program.scrtch2.length )
    assert_equal(2, program.tbl.length )
  end

  def test_dup_tables
    assert_raise(Bud::CompileError) {program = DupTableBud.new}
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
    program.tick
    lines = program.matches.to_a
    assert_equal(1, lines.length)
    assert_equal(44, lines[0][0])
  end

  def test_union
    s = Union.new
    s.tick
    assert_equal(2, s.union.length)
    assert_equal([["a", "b", 4], ["a", "b", 1]], s.union.to_a)
  end

  def test_delete_key
    d = DeleteKey.new
    d.tick
    assert_equal(1, d.t1.length)
    d.del_buf << [5, 11] # shouldn't delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
    assert_equal(1, d.t1.length)

    d.del_buf << [5, 10] # should delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
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
    p1.tick
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
    assert_raise(Bud::CompileError) { DupTableDef.new }
  end

  def test_filter_and_delete
    b = DelBug.new

    b.tick
    assert_equal([[1,2], [3,4]], b.buffer.to_a.sort)

    b.to_delete << [1,2]
    b.tick
    b.tick
    assert_equal([[3,4]], b.buffer.to_a.sort)
  end

  def test_bad_declaration
    assert_raise(Bud::CompileError) { BadDeclaration.new }
  end

  def test_declaration_override_method
    assert_raise(Bud::CompileError) { DeclarationOverrideMethod.new }
  end

  def test_empty_pk_error
    e = EmptyPk.new
    e.t1 << ["xyz", 6]
    e.tick
    e.t1 <+ [["xyz", 6]]
    e.tick
    e.t1 <+ [["xxx", 2]]
    assert_raise(Bud::KeyConstraintError) { e.tick }
  end

  def test_empty_pk_has_key
    e = EmptyPk.new
    e.tick
    assert_equal(false, e.t1.has_key?([]))
    e.t1 << ["xyz", 10]
    assert(e.t1.has_key? [])
    assert_equal(1, e.t1.length)
  end

  class SimpleRename
    include Bud
    state do
      table :t1
    end
    bootstrap do
      t1 << [1,1]
    end
    bloom do
      temp :t2 <= t1.rename(:bob)
    end
  end

  def test_simple_rename
    p = SimpleRename.new
    assert_nothing_raised {p.tick}
  end

  class FunkyPayloads
    include Bud
    state do
      channel :c1, [:pay, :morepay, :@loc]
      channel :c2, [:pay, :@loc, :morepay]
      table :t1
      table :t2
    end
    bloom do
      t1 <= c1.payloads
      t2 <= c2.payloads
    end
  end

  def test_funky_payload
    p = FunkyPayloads.new
    p.run_bg
    p.sync_callback(:c1, [["hi", "miley", p.ip_port]], :t1)
    p.sync_callback(:c2, [["guy", p.ip_port, "smiley"]], :t1)
  end
end
