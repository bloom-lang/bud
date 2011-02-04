require 'rubygems'
require 'bud'
require 'test/unit'

class BabyBud < Bud
  def state
    scratch :scrtch, ['k1', 'k2'], ['v1', 'v2']
    scratch :scrtch2, ['k1', 'k2']
    table :tbl, ['k1', 'k2'], ['v1', 'v2']
  end

  def bootstrap
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

class DupKeyBud < Bud
  def state
    scratch :tab, ['k'], ['name']
  end

  declare
  def program
    tab <= [[2000, 'bush']]
    tab <= [[2000, 'gore']]
  end
end

class DupTableBud < Bud
  def state
    scratch :s, ['k']
    scratch :s, ['l']
  end
end

class DupColBud < Bud
  def state
    scratch :silly, ['a', 'a']
  end
end

class Grep < Bud
  attr_reader :pattern

  def initialize(pattern)
    super()
    @pattern = pattern
  end

  def state
    file_reader :text, '../examples/chap2/ulysses.txt'
    table :matches, ['lineno', 'text']
  end

  declare
  def program
    matches <= text.map{|t| t if t.text =~ pattern}
  end
end

class Union < Bud
  def state
    table :link, ['from', 'to', 'cost']
    table :delta_link, ['from', 'to', 'cost']
    table :union, ['from', 'to', 'cost']
  end

  def bootstrap
    link <= [['a', 'b', 1]]
    delta_link <= [['a', 'b', 4]]
  end

  declare
  def prog
    union <= (delta_link <= link)
  end
end

class DeleteKey < Bud
  def state
    table :t1, ['k'], ['v']
    table :del_buf, ['k', 'v']
  end

  def bootstrap
    t1 << [5, 10]
  end

  declare
  def rules
    t1 <- del_buf
  end
end

class TestCollections < Test::Unit::TestCase
  def test_simple_deduction
    program = BabyBud.new
    assert_equal(2, program.scrtch.length)
    assert_equal(1, program.scrtch2.length)
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(0, program.scrtch2.length)
    assert_equal(2, program.tbl.length)
  end

  def test_tuple_accessors
    program = BabyBud.new
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
    assert_equal("[[\"a\", \"b\", 4], [\"a\", \"b\", 1]]", s.union.map{|t| t}.inspect)
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
end
