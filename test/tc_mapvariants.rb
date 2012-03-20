require './test_common'

# Check that maps over constant ranges aren't converted to semi-map
class LeaveMapAlone
  include Bud

  state do
    table :num, [:num]
  end

  bloom do
    num <= (1..5).map{|i| [i]}
  end
end

class AllMapsAreOne
  include Bud

  state do
    scratch :out, [:val]
    scratch :snout, [:val]
    scratch :clout, [:val]
    scratch :inski
  end
  
  bootstrap {inski <= [[1,1], [2,2], [3,3]]}

  bloom do
    out <= inski {|i| [i.val]}
    snout <= inski.map {|i| [i.val]}
    clout <= inski.pro {|i| [i.val]}
  end
end

class StillAnnoying
  include Bud

  state do
    scratch :out, [:val]
    scratch :inski
  end

  bloom :rules do
    temp :k <= inski
    out <= k.map {|t| [t.val]}
  end
end

class LessAnnoying < StillAnnoying
  include Bud

  bloom :rules do
    temp :tmpy <= inski
    out <= tmpy {|t| [t.val]}
  end
end

class TestMapVariants < MiniTest::Unit::TestCase
  def test_leave_map_alone
    program = LeaveMapAlone.new
    program.tick
    assert_equal([[1],[2],[3],[4],[5]], program.num.to_a.sort)
  end

  def test_all_maps
    p = AllMapsAreOne.new
    p.tick
    assert_equal(3, p.out.length)
    assert_equal(p.out.to_a, p.snout.to_a)
    assert_equal(p.out.to_a, p.clout.to_a)
  end
end

class TestProEnumerable < MiniTest::Unit::TestCase
  class SortIdAssign
    include Bud

    state do
      interface input, :in_t, [:payload]
      interface output, :out_t, [:ident] => [:payload]
    end

    bloom do
      out_t <= in_t.sort.each_with_index.map {|a, i| [i, a]}
    end
  end

  def test_sort_pro
    p = SortIdAssign.new
    p.run_bg
    r = p.sync_callback(:in_t, [[5], [1], [100], [6]], :out_t)
    assert_equal([[0, [1]], [1, [5]], [2, [6]], [3, [100]]], r.to_a.sort)
    p.stop
  end
end
