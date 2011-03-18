require 'test_common'

module ParentModule
  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [5,10]
  end

  bloom :parent_rules do
    t2 <= t1
  end
end

class ChildClass
  include Bud
  import ParentModule => :p

  state do
    table :t3, p.t2.key_cols => p.t2.val_cols
  end

  bloom do
    t3 <= p.t2
  end
end

class TestModules < Test::Unit::TestCase
  def test_simple
    c = ChildClass.new
    c.tick
    assert_equal([[5,10]], c.t3.to_a.sort)
  end
end

# Testing TODO:
# * GrandChild (class), state ref c.p.boot_t
# * ChildModule, state ref p.boot_t
# * Module table on LHS of class
# * Module table on LHS of module
# * Rule blocks in modules
