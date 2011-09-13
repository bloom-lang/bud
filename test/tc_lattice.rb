require 'test_common'

class SimpleMax
  include Bud

  state do
    max :m
    scratch :s, [:val]
    scratch :done, [:t]
  end

  bloom do
    m <= s
    done <= Max.gt_k(10) {|t| [true]}
  end
end

class MaxOfMax
  include Bud

  state do
    max :m1
    max :m2
    max :m3
    scratch :s, [:val]
    scratch :done, [:t]
  end

  bloom do
    m1 <= s {|t| t if t.val % 2 == 0}
    m2 <= s {|t| t if t.val % 2 == 1}
    m3 <= m1
    m3 <= m2
    done <= Max.gt_k(10) {|t| [true]}
  end
end

class TestMaxLattice
  def test_simple_max
    i = SimpleMax.new
    i.s <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
  end

  def test_max_of_max
  end
end
