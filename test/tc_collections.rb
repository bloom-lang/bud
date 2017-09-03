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
    t2 <= t1 {|t| [t.key + 1, t.val + 1]}
    t3 <= t2 {|t| [t.key + 1, t.val + 1]}
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
    t3 <= t1 {|t| t if t2.include? t}
    t4 <= t1 {|t| t if t2.has_key? [t.k]}
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

  bloom do
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

class StructArrayConcat
  include Bud

  state do
    table :t1
    table :t2, [:a, :b, :c]
    table :t3
    table :t4, [:a, :b, :c, :d]
  end

  bloom do
    t2 <= t1 {|t| t + [5]}
    t2 <= t1 {|t| [9] + t}
    t4 <= (t1 * t3).pairs {|x,y| x + y}
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
    assert_equal([["c", "d", 5, 6], ["z", "y", 9, 8]],
                 program.tbl.sort)
    assert_equal([["z", "y", 9, 8], ["c", "d", 5, 6]],
                 program.tbl.sort {|x,y| y <=> x})
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
    # Make sure that file_reader works in background
    q = Queue.new
    program.register_callback(:matches) { q.push(true) }
    program.run_bg
    q.pop
    program.stop_bg

    lines = program.matches.to_a
    assert_equal(1, lines.length)
    assert_equal(44, lines[0][0])
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
    d.del_buf <+ [[5, 11]] # shouldn't delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
    assert_equal(1, d.t1.length)

    d.del_buf <+ [[5, 10]] # should delete
    d.tick
    assert_equal(1, d.t1.length)
    d.tick
    assert_equal(0, d.t1.length)
  end

  def test_row_equality
    rv = RowValueTest.new
    rv.t1 <+ [[5, 10],
              [6, 11]]
    rv.t2 <+ [[5, 10],
              [6, 15],
              [7, 12]]
    rv.tick

    assert_equal(1, rv.t3.length)
    assert_equal(2, rv.t4.length)

    cnt = rv.t4.select {|t| t == [5, 10, 15]}
    assert_equal([], cnt)
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
      t1 << [[5, nil]]
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
      p.t2 <+ [[[5, nil], nil]]
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
    assert_raises(Bud::CompileError) { LocSpecNonChannel.new }
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

  def test_empty_pk_include
    e = EmptyPk.new
    e.t1 <+ [["x", "y"]]
    e.tick
    assert(!e.t1.include?(["x", "z"]))
    assert(e.t1.include?(["x", "y"]))
  end

  def test_periodic_lhs_error
    b = InsertIntoPeriodicError.new
    b.run_bg
    assert_raises(Bud::CompileError) {
      b.sync_do { b.timer <+ [[5, 10]] }
    }
    assert_raises(Bud::CompileError) {
      b.sync_do { b.timer <= [[5, 10]] }
    }
    assert_raises(Bud::CompileError) {
      b.sync_do { b.timer <- [[5, 10]] }
    }
    b.stop
  end

  class StdioPendingInsert
    include Bud

    state { table :t1 }

    bloom do
      stdio <+ t1 {|t| ["Foo: #{t}"]}
    end
  end

  def test_stdio_pending_insert
    b = StdioPendingInsert.new
    assert_raises(Bud::CompileError) { b.tick }
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

  def test_struct_array_concat
    s = StructArrayConcat.new
    s.t1 <+ [[5, 10], [6, 12]]
    s.t3 <+ [[7, 8]]
    s.tick
    assert_equal([[5, 10, 5], [6, 12, 5], [9, 5, 10], [9, 6, 12]].sort,
                 s.t2.to_a.sort)
    assert_equal([[5, 10, 7, 8], [6, 12, 7, 8]], s.t4.to_a.sort)
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

  class TooManyColumns
    include Bud

    state do
      table :t1
      scratch :s1
    end

    bloom do
      t1 <+ s1 {|s| [s.key, s.key, s.val]}
    end
  end

  def test_too_many_columns
    b = TooManyColumns.new
    assert_raises(Bud::TypeError) { b.t1 <+ [[1, 5], [1, 2, 3]] }
  end

  def test_too_many_columns_rule
    b = TooManyColumns.new
    b.s1 <+ [[10, 20]]
    assert_raises(Bud::TypeError) { b.tick }
  end

  class PadMissingField
    include Bud

    state do
      scratch :in_t, [:v]
      scratch :out_t, [:v1, :v2]
    end

    bloom do
      out_t <= in_t
    end
  end

  def test_pad_missing_field
    i = PadMissingField.new
    i.in_t <+ [[5]]
    i.tick
    assert_equal([[5, nil]], i.out_t.to_a)
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

  def test_instant_merge_outside_bud
    program = BabyBud.new
    program.tbl <+ [['a', 'b', 'c', 'd']]
    program.scrtch <+ [['a', 'b', 'c', 'd']]
    assert_raises(Bud::CompileError) { program.tbl <= [['a', 'b', 'c', 'd']] }
    assert_raises(Bud::CompileError) { program.scrtch <= [['a', 'b', 'c', 'd']] }
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

class ScratchTickDeltas < MiniTest::Unit::TestCase
  # issue 281
  class AccumDeltasInScratch
    include Bud

    state do
      scratch :l
      scratch :r
      table :lside
      table :rside
      scratch :result
      scratch :result_s, [:v]
    end

    bloom do
      lside <= l
      rside <= r
      result <= (lside * rside).lefts(:val => :val)
      result_s <= result.inspected
    end
  end

  def test_scratch_accum_tick_deltas
    i = AccumDeltasInScratch.new
    %w[l r lside rside result].each do |r|
      assert_equal(0, i.collection_stratum(r))
    end
    %w[result_s].each do |r|
      assert_equal(1, i.collection_stratum(r))
    end
    i.tick
    i.l <+ [[1, 2]]
    i.r <+ [[1, 2]]
    i.tick
    assert_equal([["[1, 2]"]], i.result_s.to_a)
  end
end

class TestEachWithIndex < MiniTest::Unit::TestCase
  class UseEachWithIndex
    include Bud

    state do
      scratch :in_t, [:v]
      table :res_t, [:nest_v, :idx]
      table :res_t_block, [:v, :idx]
    end

    bloom do
      res_t <= in_t.each_with_index
      res_t_block <= in_t.each_with_index {|t, i| [t.v, i + 100]}
    end
  end

  # Test that collection methods behave consistently outside Bud
  def test_outside_bud
    i = UseEachWithIndex.new
    i.in_t <+ [[14]]
    i.tick

    i.in_t.each_with_index do |r, idx|
      assert_equal([14], r)
      assert_equal(0, idx)
    end
    i.in_t.each do |r|
      assert_equal([14], r)
    end
    map_rv = i.in_t.map {|t| [t.v + 1]}
    assert_equal([[15]], map_rv)

    i.in_t <+ [[12]]
    pro_rv = i.in_t.pro {|t| [t.v + 1] if t.v != 12}
    assert_equal([[15]], pro_rv)
  end

  def test_each_with_index
    i = UseEachWithIndex.new
    i.in_t <+ [[8]]
    i.tick
    i.in_t <+ [[9]]
    i.tick
    assert_equal([[[8], 0], [[9], 0]], i.res_t.to_a.sort)
    assert_equal([[8, 100], [9, 100]], i.res_t_block.to_a.sort)

    i.in_t <+ [[4], [5], [6]]
    i.tick
    new_res = i.res_t.to_a.select {|t| t.nest_v.first <= 6}
    assert_equal([4, 5, 6].to_set, new_res.map {|t| t.nest_v.first}.to_set)
    assert_equal([0, 1, 2].to_set, new_res.map {|t| t.idx}.to_set)

    new_res_b = i.res_t_block.to_a.select {|t| t.v <= 6}
    assert_equal([4, 5, 6].to_set, new_res_b.map {|t| t.v}.to_set)
    assert_equal([100, 101, 102].to_set, new_res_b.map {|t| t.idx}.to_set)
  end
end

class TestBudStruct < MiniTest::Unit::TestCase
  MyStruct = Bud::TupleStruct.new(:x, :y)

  def test_struct_hashing
    v1 = MyStruct.new(1, 2)
    v2 = MyStruct.new(1, 2)
    assert_equal(v1, v2)
    assert_equal([1, 2], v1)

    h = {}
    h[v1] = 1
    assert(h.has_key? v2)
  end

  def test_struct_comparable
    v1 = MyStruct.new(1, 2)
    v2 = MyStruct.new(1, 3)
    v3 = MyStruct.new(5, 1)
    v4 = MyStruct.new(9, 0)

    assert_equal(0, v1 <=> v1)
    assert_equal(-1, v1 <=> v2)
    assert_equal(1, v2 <=> v1)
    assert(v1 < v2)
    assert(v2 > v1)
    assert_equal([v1, v2, v3, v4], [v2, v4, v3, v1].sort)
  end
end
