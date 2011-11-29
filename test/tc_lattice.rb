require 'test_common'

class SimpleMax
  include Bud

  state do
    lmax :m
    lbool :done
  end

  bloom do
    done <= m.gt(10)
  end
end

class TestMax < Test::Unit::TestCase
  def test_simple
    i = SimpleMax.new
    i.m <= [[5], [10]]
    i.tick
    assert_equal(false, i.done.reveal)
  end
end
