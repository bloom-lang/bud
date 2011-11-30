require 'test_common'

class SimpleMax
  include Bud

  state do
    lmax :m
    lbool :done
  end

  bloom do
    done <= m.gt(12)
  end
end

class TestMax < Test::Unit::TestCase
  def test_simple
    i = SimpleMax.new
    i.m <+ [[5], [10]]
    i.tick
    assert_equal(false, i.done.reveal)
    i.m <+ [[15]]
    i.tick
    assert_equal(true, i.done.reveal)
  end
end
