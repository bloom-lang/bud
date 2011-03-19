require 'test_common'

module ParentModule
  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [5, 10]
  end

  bloom :parent_rules do
    t2 <= t1
  end

  def t1_val_sum
    t1.values.flatten.reduce(:+)
  end
end

class ChildClass
  include Bud
  import ParentModule => :p

  state do
    table :t3, p.t2.key_cols => p.t2.val_cols
  end

  bootstrap do
    p.t2 << [200, 400]
    p.t1 <= [[500, 1000]]
  end

  # Shouldn't override the named block declared in ParentModule
  bloom :parent_rules do
    t3 <= p.t2
  end

  def get_sum
    p.t1_val_sum
  end
end

class ChildImportTwice
  import ParentModule => :x
  import ParentModule => :y
  include Bud

  state do
    table :t4, x.t1.key_cols => y.t2.val_cols
    table :t5, y.t1.key_cols => x.t2.val_cols
  end

  bootstrap do
    y.t1 << [50,100]
  end

  bloom do
    t4 <= y.t2.map {|t| [t.key + 10, t.val + 10]}
    t5 <= x.t2
  end
end

module ChildModule
  import ParentModule => :p
end

class GrandChildClass
  include Bud
  import ChildModule => :c
  import ParentModule => :p

  state do
    table :t6, c.p.t1.key_cols => c.p.t2.val_cols
    table :t7
  end

  bloom do
    t6 <= c.p.t1
    t7 <= p.t2.map {|p| [p.key + 20, p.val + 20]}
  end
end

class TestModules < Test::Unit::TestCase
  def test_simple
    c = ChildClass.new
    c.tick
    assert_equal([[5, 10], [200, 400], [500, 1000]], c.t3.to_a.sort)
    assert_equal(1010, c.get_sum)
  end

  def test_import_twice
    c = ChildImportTwice.new
    c.tick
    assert_equal([[15, 20], [60, 110]], c.t4.to_a.sort)
    assert_equal([[5, 10]], c.t5.to_a.sort)
  end

  def test_nested_import
    c = GrandChildClass.new
    c.tick
    assert_equal([[5, 10]], c.t6.to_a.sort)
    assert_equal([[25, 30]], c.t7.to_a.sort)
  end
end

# Testing TODO:
# * GrandChild (class), state ref c.p.boot_t
# * ChildModule, state ref p.boot_t
# * Module table on LHS of module
# * Temp collections in modules (+ in classes)
# * Qualified names in (a)sync_do
# * Rename instance variables in modules?
