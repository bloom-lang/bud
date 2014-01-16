require './test_common'

class SimpleStrat
  include Bud

  state do
    table :t1
    table :t2
    table :t3
  end

  bloom do
    t2 <= t1 {|t| [t.key + 1, t.val + 1]}
    t3 <= t2.group([:key], max(:val))
  end
end

class PartHierarchy
  include Bud

  state do
  end

  bloom do
  end
end

class TestStrat < MiniTest::Unit::TestCase
  def test_simple_strat
    s = SimpleStrat.new
  end

  def test_part_hierarchy_manual_strat
  end
end
