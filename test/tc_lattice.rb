require 'test_common'

class IMax
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

class TestMaxLattice
  def test_imax
    i = IMax.new
    i.s <+ [[1], [2], [3]]
    i.tick
    assert(i.done.empty?)
  end
end

