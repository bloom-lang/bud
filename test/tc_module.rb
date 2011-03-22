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

  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [1000, 2000]
  end

  bloom do
    t2 <= t1.map {|t| [t.key + 7, t.val + 7]}
  end
end

class GrandChildClass
  include Bud
  import ChildModule => :c
  import ParentModule => :p

  state do
    table :t6, c.p.t1.key_cols => c.p.t2.val_cols
    table :t7
    table :t8
  end

  bootstrap do
    c.t2 << [0, 0]
  end

  bloom do
    t6 <= c.p.t1
    t7 <= p.t2.map {|p| [p.key + 20, p.val + 20]}
    t8 <= c.t2
  end
end

# Issue #109
module InterfaceMod
  state do
    interface input, :iin
    interface output, :iout
  end

  bloom do
    iout <= iin
  end
end

class InterfaceModUser
  include Bud
  import InterfaceMod => :m

  state do
    table :t1
  end

  bootstrap do
    m.iin <= [[35, 45]]
  end

  bloom do
    t1 <= m.iout
  end
end

# Issue #110
module ModuleA
  state do
    table :t1
    table :t2
  end

  bloom do
    t1 <= t2
  end
end

module ModuleB
  state do
    table :t3
  end

  bloom do
    t1 <= t3
  end
end

class Issue110
  include Bud
  include ModuleA
  include ModuleB

  bootstrap do
    t2 << [400, 500]
    t3 << [100, 200]
  end
end

# Check that ordering dependencies (implied by the inheritance hierarchy between
# modules) are respected after module import.
module ModuleA1
  state do
    table :t1
  end
end
module ModuleA2
  state do
    table :t2
  end
end
module ModuleB1
  include ModuleA1

  state do
    table :t3, t1.schema
  end
end
module ModuleC1
  include ModuleB1
  include ModuleA2

  state do
    table :t4, t2.schema
    table :t5, t1.schema
  end
end

class ModuleStateOrdering
  include Bud
  import ModuleC1 => :m
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
    assert_equal([[0, 0], [1007, 2007]], c.t8.to_a.sort)
  end

  def test_interface_module
    c = InterfaceModUser.new
    c.tick
    assert_equal([[35, 45]], c.t1.to_a.sort)
  end

  def test_issue110
    c = Issue110.new
    c.tick
    assert_equal([[100, 200], [400, 500]], c.t1.to_a.sort)
  end

  def test_module_import_state_order
    c = ModuleStateOrdering.new
    c.tick
  end

  module OuterModule
    module NestedModule
      state do
        table :x
        table :y
      end

      bootstrap do
        x << [30, 40]
        y << [50, 60]
      end
    end
  end

  class NestedModuleUser
    include Bud
    import OuterModule::NestedModule => :nm

    state do
      table :z
    end

    bloom do
      z <= nm.x
      z <= nm.y
    end
  end

  def test_nested_module_import
    c = NestedModuleUser.new
    c.tick
    assert_equal([[30, 40], [50, 60]], c.z.to_a.sort)
  end

  def test_duplicate_import
    assert_raise(Bud::CompileError) do
      eval "
      class DupImport
        include Bud
        import ParentModule => :p
        import ParentModule => :p
      end"
    end
  end

  # ParseTree failed for methods defined in "grandparent" modules.
  module ModuleC
    def foo; puts "hello, world"; end
    def bar; puts "baz"; end
  end
  module ModuleD
    include ModuleC
  end
  module ModuleE
    include ModuleD
  end

  def test_parsetree_bug
    assert_nothing_raised do
      eval "
      class DescendentClass
        import ModuleE => :e
        include Bud
      end
      "
    end
  end

  def test_import_class
    assert_raise(Bud::BudError) do
      eval "
      class DummyClass; end
      class DummyImporter
        include Bud
        import DummyClass => :c
      end"
    end
  end
end

# Testing TODO:
# * Temp collections in modules (+ in classes)
# * Qualified names in (a)sync_do
# * Rename instance variables in modules?
