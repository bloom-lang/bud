require 'test_common'

module ParentModule
  include BudModule

  state do
    table :boot_t
  end

  bootstrap do
    boot_t << [5, 10]
    boot_t << [20, 30]
  end
end

class ImportParent
  include Bud
  import ParentModule => :p

  state do
    table :t2
  end

  declare
  def do_child
    t2 <= p.boot_t.map {|t| [t.key + 1, t.val + 1]}
  end
end

class TestModules < Test::Unit::TestCase
  def test_simple_bootstrap
    c = ImportParent.new
    c.tick
    assert_equal([[6, 11], [21, 31]], c.t2.to_a.sort)
  end
end

