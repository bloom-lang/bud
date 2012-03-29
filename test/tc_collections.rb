require './test_common'

class BabyBud
  include Bud

  state do
    scratch :scrtch, [:k1, :k2] => [:v1, :v2]
    scratch :scrtch2, [:k1, :k2]
    table :tbl, [:k1, :k2] => [:v1, :v2]
    scratch :the_keys
    scratch :the_vals
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
    the_keys <= tbl.keys
    the_vals <= tbl.values
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
    file_reader :txt, 'text/ulysses.txt'
    table :matches, [:lineno, :text]
    table :text_out, [:lineno, :text]
  end

  bloom do
    text_out <= txt
    matches <= txt {|t| t if t.text =~ pattern}
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

class StringMerge
  include Bud

  state do
    table :t1
  end

  bloom do
    t1 <= ["hello", "world"]
  end
end

class StringAsyncMerge
  include Bud

  state do
    table :t1
  end

  bootstrap do
    t1 <= [[5, 10]]
  end

  bloom do
    stdio <~ t1 { "hello, world" }
  end
end

class DupTableDef
  include Bud

  state do
    table :t1
    scratch :t1
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
    scratch :object_id
  end
end

class LocSpecNonChannel
  include Bud
  state do
    scratch :foo, [:@a] => [:b]
  end
end

class EmptyPk
  include Bud

  state do
    table :t1, [] => [:foo, :bar]
  end

  bloom :dummy do
    t1 <= t1 # to force evaluation
  end
end

class InsertIntoPeriodicError
  include Bud

  state do
    periodic :timer
  end
end

class SchemaPreserveKeys
  include Bud

  state do
    scratch :inputt, [:a, :b]
    scratch :t1, [:a] => [:b]
    scratch :t2, t1.schema
  end

  bloom do
    t1 <= inputt
    t2 <= inputt
  end
end

class TestCollections < MiniTest::Unit::TestCase
  def test_simple_deduction
    program = BabyBud.new
    program.tick
    assert_equal(1, program.scrtch2.length)
    program.tick
    assert_equal([["c", "d", 5, 6]], program.scrtch.to_a)
    assert_equal(0, program.scrtch2.length)
    assert_equal(2, program.tbl.length)
    assert_equal([["c", "d"], ["z", "y"]], program.the_keys.to_a.sort)
    assert_equal([[5,6], [9,8]], program.the_vals.to_a.sort)
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
    assert_equal(1, program.scrtch.length)
    assert_equal(0, program.scrtch2.length)
    assert_equal(2, program.tbl.length)
  end

  def test_dup_tables
    assert_raises(Bud::CompileError) {program = DupTableBud.new}
  end

  def test_dup_columns
    assert_raises(Bud::Error) {program = DupColBud.new}
  end

  def test_dup_keys
    program = DupKeyBud.new
    assert_raises(Bud::KeyConstraintError) { program.tick }
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
    assert_equal([["a", "b", 1], ["a", "b", 4]], s.union.to_a.sort)
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

  def test_delete_key
    d = DeleteKey.new
    d.tick
    assert_equal(1, d.t1.length)
    d.del_buf <= [[5, 11]] # shouldn't delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
    assert_equal(1, d.t1.length)

    d.del_buf <= [[5, 10]] # should delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
    assert_equal(0, d.t1.length)
  end

  def test_row_equality
    rv = RowValueTest.new
    rv.run_bg
    rv.sync_do {
      rv.t1 <+ [[5, 10],
                [6, 11]]
      rv.t2 <+ [[5, 10],
                [7, 12],
                [6, 15]]
    }

    rv.sync_do {
      assert_equal(1, rv.t3.length)
      assert_equal(2, rv.t4.length)
    }

    rv.stop
  end

  def test_types
    p1 = BendTypes.new
    p1.tick
    assert_equal(1, p1.t1.first.key)
    p2 = NonEnumerable.new
    assert_raises(Bud::TypeError) { p2.tick }
    p3 = NonTuple.new
    assert_raises(Bud::TypeError) { p3.tick }
    p4 = NonTupleDelete.new
    assert_raises(Bud::TypeError) { p4.tick }
    p5 = StringMerge.new
    assert_raises(Bud::TypeError) { p5.tick }
    p6 = StringAsyncMerge.new
    assert_raises(Bud::TypeError) { p6.tick }
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
      t1 <- t2 {|t| [t.k1]}
    end
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
      assert_equal([[5, 10]], p.t1.to_a)
    }
    p.stop
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

    b.stop
  end

  def test_dup_table_def
    assert_raises(Bud::CompileError) { DupTableDef.new }
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
      buffer <- to_delete {|t| t if t.val != 4}
    end
  end

  def test_filter_and_delete
    b = DelBug.new
    b.tick
    assert_equal([[1,2], [3,4]], b.buffer.to_a.sort)

    b.to_delete <+ [[1,2]]
    b.tick
    b.tick
    assert_equal([[3,4]], b.buffer.to_a.sort)
  end

  def test_bad_declaration
    assert_raises(Bud::CompileError) { BadDeclaration.new }
  end

  def test_declaration_override_method
    assert_raises(Bud::CompileError) { DeclarationOverrideMethod.new }
  end

  def test_loc_spec_non_channel
    assert_raises(Bud::Error) { LocSpecNonChannel.new }
  end

  def test_empty_pk_error
    e = EmptyPk.new
    e.t1 <+ [["xyz", 6]]
    e.tick
    e.t1 <+ [["xyz", 6]]
    e.tick
    e.t1 <+ [["xxx", 2]]
    assert_raises(Bud::KeyConstraintError) { e.tick }
  end

  def test_empty_pk_has_key
    e = EmptyPk.new
    e.tick
    assert_equal(false, e.t1.has_key?([]))
    e.t1 << ["xyz", 10]
    assert(e.t1.has_key? [])
    assert_equal(1, e.t1.length)
  end

  def test_periodic_lhs_error
    b = InsertIntoPeriodicError.new
    b.run_bg
    assert_raises(Bud::Error) {
      b.sync_do { b.timer <+ [[5, 10]] }
    }
    assert_raises(Bud::Error) {
      b.sync_do { b.timer <= [[5, 10]] }
    }
    assert_raises(Bud::Error) {
      b.sync_do { b.timer <- [[5, 10]] }
    }
    b.stop
  end

  class SimpleRename
    include Bud
    state do
      table :t1
    end
    bootstrap do
      t1 <= [[1,1], [1000, 1000]]
    end
    bloom do
      temp :t2 <= t1.rename(:bob, [:a] => [:b]) {|t| t if t.a < 1000}
    end
  end

  def test_simple_rename
    p = SimpleRename.new
    p.tick
    assert_equal(:t2, p.t2.tabname)
    assert_equal([[1, 1]], p.t2.to_a)
  end

  def test_schema_preserve_keys
    s = SchemaPreserveKeys.new
    assert_equal({[:a] => [:b]}, s.t2.schema)
    s.inputt <+ [[5, 10], [5, 11]]
    assert_raises(Bud::KeyConstraintError) { s.tick }
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

class TestUpsert < MiniTest::Unit::TestCase
  class UpsertTest
    include Bud
    state do
      table :joe
      table :t2
      scratch :t1d
      scratch :t2d
    end
    bootstrap do
      joe << [1,'a']
      t2 << [2,'x']
      t1d << [1,'b']
      t2d << [2,'y']
    end
    bloom do
      joe <+- t1d
      t2 <-+ t2d
    end
  end

  def test_upsert
    p = UpsertTest.new
    p.tick
    assert_equal([[1,'a']], p.joe.to_a)
    assert_equal([[2,'x']], p.t2.to_a)
    p.tick
    assert_equal([[1,'b']], p.joe.to_a)
    assert_equal([[2,'y']], p.t2.to_a)
  end
end
  
class TestTransitivity < MiniTest::Unit::TestCase
  class TransitivityTest
    include Bud
    state do
      scratch :t1, [:a, :b]
      scratch :t2, [:c, :d]
      scratch :t3, [:e, :f]
    end
    bootstrap do
      t1 << [1,1]
    end
    bloom do
      t2 <= t1
      t3 <= t2
    end
  end
  
  def test_transitivity
    p = TransitivityTest.new
    p.tick
    assert_equal([[1,1]], p.t1.to_a)
    assert_equal([[1,1]], p.t2.to_a)
    assert_equal([[1,1]], p.t3.to_a)
  end
end

class TestCollExpr < MiniTest::Unit::TestCase
  class CollExprTest
    include Bud
    state do 
      table :his
      coll_expr :e, lambda {[[budtime, 'hi']]}
    end
    bloom do
      his <= e
    end
  end
  
  def test_coll_expr
    p = CollExprTest.new
    3.times { p.tick }
    assert_equal([[0,'hi'],[1,'hi'],[2,'hi']], p.his.to_a.sort)
  end
end

class TestConstants < MiniTest::Unit::TestCase
  class StrOut
    include Bud
    state { table :his}
    bloom do
      his <= [[budtime, 'hi']]
    end
  end
  
  def test_str_out
    p = StrOut.new
    3.times { p.tick }
    assert_equal([[0,'hi'],[1,'hi'],[2,'hi']], p.his.to_a.sort)
  end
end
