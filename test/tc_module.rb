require './test_common'

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
    t1.to_a.map{|t| t[1]}.reduce(:+)
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
  import ParentModule => :xx
  import ParentModule => :yy
  include Bud

  state do
    table :t4, xx.t1.key_cols => yy.t2.val_cols
    table :t5, yy.t1.key_cols => xx.t2.val_cols
  end

  bootstrap do
    yy.t1 << [50,100]
  end

  bloom do
    t4 <= yy.t2.map {|t| [t.key + 10, t.val + 10]}
    t5 <= xx.t2
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

module BaseInsertT2
  state do
    table :t1
    table :t2
    table :t3
  end

  bootstrap do
    t1 <= [[10, 20], [40, 50]]
  end

  bloom :foo do
    t2 <= t1
  end
end

module DoInsertT3
  include BaseInsertT2

  bloom :foo do
    t3 <= t1
  end
end

class ModuleMethodOverride
  include Bud
  import DoInsertT3 => :m

  def do_check
    sync_do {
      raise unless m.t2.empty?
      raise unless m.t3.to_a.sort == [[10, 20], [40, 50]]
    }
  end
end

module TempMod
  state do
    table :t1
    table :t2
  end

  bloom do
    temp :t3 <= t1 {|t| [t.key + 20, t.val + 20]}
    t2 <= t3 {|t| [t[0] + 10, t[1] + 10]}
  end
end

class TempModUser
  include Bud
  import TempMod => :m1
  import TempMod => :m2

  bootstrap do
    m1.t1 << [10, 10]
    m2.t1 << [20, 20]
  end

  def do_check
    raise unless m1.t2.to_a.sort == [[40, 40]]
    raise unless m2.t2.to_a.sort == [[50, 50]]
  end
end

module ModDefMethod
  state do
    table :t1
  end

  def one
    1
  end

  bloom do
    t1 <= [[one(), one + 10]]
  end
end

class UseModDefMethod
  include Bud
  import ModDefMethod => :mdm

  def do_check
    raise unless mdm.t1.to_a.sort == [[1, 11]]
  end
end

class TestModules < MiniTest::Unit::TestCase
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
    assert_equal(c.t1.to_a.sort, [[35, 45]])
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

  module ModuleT1
    state do
      table :t1
    end
  end

  module IncludeT1
    import ModuleT1 => :m

    state do
      table :t2, m.t1.schema
    end

    bloom :logic do
      m.t1 <= t2
    end
  end

  class IncludeT1User
    include Bud
    import IncludeT1 => :t

    bootstrap do
      t.t2 <= [[35, 70], [45, 90]]
    end

    def do_check
      sync_do {
        raise unless [[35, 70], [45, 90]] == t.m.t1.to_a.sort
      }
    end
  end

  def test_nested_ref_import
    t = IncludeT1User.new
    t.run_bg
    t.do_check
    t.stop
  end

  module OuterModule
    module NestedModule
      state do
        table :xx
        table :yy
      end

      bootstrap do
        xx << [30, 40]
        yy << [50, 60]
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
      z <= nm.xx
      z <= nm.yy
    end
  end

  def test_nested_module_import
    c = NestedModuleUser.new
    c.tick
    assert_equal([[30, 40], [50, 60]], c.z.to_a.sort)
  end

  def test_anon_class
    k = Class.new do
      include Bud

      state do
        table :t1
        scratch :s1
      end

      bloom do
        t1 <= s1
      end
    end
    c = k.new
    c.run_bg
    c.sync_do {
      c.s1 <+ [[5, 10]]
    }
    c.sync_do {
      assert_equal([[5, 10]], c.t1.to_a.sort)
    }
    c.stop
  end

  def test_module_method_override
    c = ModuleMethodOverride.new
    c.run_bg
    c.sync_do
    c.do_check
    c.stop
  end

  def test_import_shadow_method
    assert_raises(Bud::CompileError) do
      eval %{
        class ImportShadowMethod
          include Bud
          def foo; end
          import ModuleC1 => :foo
        end
      }
    end
  end

  def test_module_temp_collection
    c = TempModUser.new
    c.tick
    c.do_check
  end

  def test_module_def_method
    c = UseModDefMethod.new
    c.tick
    c.do_check
  end

  def test_duplicate_import
    assert_raises(Bud::CompileError) do
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
    eval %{
      class DescendentClass
        import ModuleE => :e
        include Bud
      end
    }
  end

  def test_import_class
    assert_raises(Bud::CompileError) do
      eval %{
      class DummyClass; end
      class DummyImporter
        include Bud
        import DummyClass => :c
      end
      }
    end
  end

  module IncludesBud
    include Bud
    state { table :t1 }
  end

  module IncludesBudInParent
    include IncludesBud
    state { table :t2 }
  end

  def Xtest_import_bud_module
    # We can't safely import a module that has already included Bud
    assert_raises(Bud::CompileError) do
      eval "
      class TestImportOfIncludesBud
        include Bud
        import IncludesBud => :b
      end"
    end

    assert_raises(Bud::CompileError) do
      eval "
      class TestImportOfIncludesBudInParent
        include Bud
        import IncludesBudInParent => :p
      end"
    end
  end
end

# Test the interaction between the include and import statements. Specifically,
# if module Y imports Z as "z" and X includes Y, code in X can use the "z"
# qualified name.
module RootMod
  state do
    table :t, [:k]
  end
end

module ImportRoot
  import RootMod => :r

  bootstrap do
    r.t << [5]
  end
end

module IncludeImportRoot
  include ImportRoot

  bootstrap do
    r.t << [10]
  end
end

class IncludeImportUser
  include Bud
  include IncludeImportRoot

  state do
    table :t_copy, r.t.schema
  end

  bootstrap do
    r.t << [15]
  end

  bloom do
    t_copy <= r.t
  end
end

# Similar test to the above, except that there's an additional import, and hence
# the rewriting must be done earlier.
module ImportIncludeImportRoot
  import IncludeImportRoot => :i
end

class UserViaImport
  include Bud
  include ImportIncludeImportRoot

  state do
    table :t_copy, i.r.t.schema
  end

  bootstrap do
    i.r.t << [25]
  end

  bloom do
    t_copy <= i.r.t
  end
end

# Check that importing the same module name amongst included modules results in
# merging the two modules into a single namespace.
module OtherRoot
  state do
    table :xx, [:v]
  end
end

module OtherMod
  import OtherRoot => :r

  bootstrap do
    r.xx <= [[100], [200]]
  end
end

class DupImportNameDiffModule
  include Bud
  include IncludeImportRoot
  include OtherMod

  state do
    table :r_t_copy, r.t.schema
    table :r_xx_copy, r.xx.schema
  end

  bloom do
    r.xx <= r.t
    r_t_copy <= r.t
    r_xx_copy <= r.xx
  end
end

class TestIncludeImport < MiniTest::Unit::TestCase
  def test_include_import
    b = IncludeImportUser.new
    b.tick
    assert_equal([[5], [10], [15]], b.t_copy.to_a.sort)
  end

  def XXXtest_include_dup_import_name
    b = DupImportNameDiffModule.new
    b.tick
    assert_equal([[5], [10]], b.r.t_copy.to_a.sort)
    assert_equal([[5], [10], [100], [200]], b.r_xx_copy.to_a.sort)
  end

  def test_include_via_import
    b = UserViaImport.new
    b.tick
    assert_equal([[5], [10], [25]], b.t_copy.to_a.sort)
  end
end

# Check that empty named Bloom blocks are not considered no-ops for method
# overriding purposes.
class ParentBlockC
  include Bud

  state do
    table :t1
    table :t2
  end

  bootstrap do
    t2 <= [[10, 20]]
  end

  bloom :foo do
    t1 <= t2
  end
end

class ChildBlockC < ParentBlockC
  bloom :foo do
  end
end

class TestIncludeOverride < MiniTest::Unit::TestCase
  def test_override_empty
    b = ChildBlockC.new
    b.tick
    assert_equal([], b.t1.to_a)
  end
end

# Check that joins defined inside modules work correctly
class TestJoinInModule < MiniTest::Unit::TestCase
  module NestedMultiJoinDef
    state do
      scratch :t1
      scratch :t2
      scratch :t3
      scratch :res, [:a, :b, :c, :d, :e, :f]
    end

    bootstrap do
      t1 <= [[10, 20], [1, 2]]
      t2 <= [[50, 10], [1, 3]]
      t3 <= [[99, 50], [1, 4]]
    end

    bloom do
      res <= (t1 * t2 * t3).combos(t1.key => t2.val, t2.key => t3.val) do |a, b, c|
        [a.key, a.val, b.key, b.val, c.key, c.val]
      end
    end
  end

  class MultiJoinUser
    include Bud
    import NestedMultiJoinDef => :n

    state do
      scratch :res, n.res.schema
    end

    bloom do
      res <= n.res
    end
  end

  def test_multi_join
    j = MultiJoinUser.new
    j.tick
    assert_equal([[10, 20, 50, 10, 99, 50]], j.res.to_a)
  end

  module NestedBinaryJoin
    state do
      table :t1
      table :t2
      table :t3
      table :t4
      table :t5
    end

    bootstrap do
      t1 <= [[3, 4], [7, 8], [11, 17]]
      t2 <= [[3, 10], [5, 11], [11, 20]]
    end

    bloom do
      t3 <= (t1 * t2).pairs(:key => :key) {|x,y| [x.key, x.val + y.val]}
      t4 <= (t1 * t2).lefts(:key => :key)
      t5 <= (t1 * t2).rights(:key => :key)
    end
  end

  class BinaryJoinUser
    include Bud
    import NestedBinaryJoin => :n

    state do
      scratch :t3_copy, n.t3.schema
      scratch :t4_copy, n.t4.schema
      scratch :t5_copy, n.t5.schema
    end

    bloom do
      t3_copy <= n.t3
      t4_copy <= n.t4
      t5_copy <= n.t5
    end
  end

  def test_binary_join
    j = BinaryJoinUser.new
    j.tick
    assert_equal([[3, 14], [11, 37]], j.t3_copy.to_a.sort)
    assert_equal([[3, 4], [11, 17]], j.t4_copy.to_a.sort)
    assert_equal([[3, 10], [11, 20]], j.t5_copy.to_a.sort)
  end
end

class JoinModQualifier
  include Bud
  import RootMod => :r

  bootstrap do
    r.t <= [[5]]
  end

  state do
    table :t
    table :x1
    table :x2
  end

  bloom do
    x1 <= (t * r.t).pairs(:key => :k) {|t1,t2| [t1.val, t2.k]}
    x2 <= (t * r.t).rights(:key => :k) {|t2| [t2.k, 3]}
  end
end

class TestJoinWithModuleQualifier < MiniTest::Unit::TestCase
  def test_mod_join
    b = JoinModQualifier.new
    b.t <+ [[5, 10], [6, 11]]
    b.tick
    assert_equal([[10,5]], b.x1.to_a)
    assert_equal([[5,3]], b.x2.to_a)
  end
end
