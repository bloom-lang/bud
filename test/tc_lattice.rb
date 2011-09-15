require 'test_common'

class SimpleMax
  include Bud

  state do
    lat_max :m
    scratch :inputt, [:val]
    scratch :done, [:t]
    scratch :done_copy, done.schema
  end

  bloom do
    m <= inputt
    done <= m.gt_k(10) { [[true]] }
    done_copy <= done
  end
end

# class MaxOfMax
#   include Bud

#   state do
#     lat_max :m1
#     lat_max :m2
#     lat_max :m3
#     scratch :s, [:val]
#     scratch :done, [:t]
#   end

#   bloom do
#     m1 <= s {|t| t if t.val % 2 == 0}
#     m2 <= s {|t| t if t.val % 2 == 1}
#     m3 <= m1
#     m3 <= m2
#     done <= Max.gt_k(10) {|t| [true]}
#   end
# end

class TestMaxLattice < Test::Unit::TestCase
  def test_simple_max
    i = SimpleMax.new
    assert_equal(2, i.strata.length)
    strat_content = i.stratum_collection_map[0]
    assert(strat_content.include? :m)
    assert(strat_content.include? :done)
    i.inputt <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
    i.inputt <+ [[12]]
    i.tick
    assert_equal(false, i.done.empty?)
  end

  def test_max_reveal_nm
  end

  def test_max_of_max
  end
end
