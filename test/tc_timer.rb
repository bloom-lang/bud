require 'test_common'

class TemporalBudTest
  include Bud

  state do
    periodic :tik, 0.1
    table :log, tik.schema
  end

  bloom do
    log <= tik {|t| [t.key, t.val - 100]}
  end
end

class TestTimer < Test::Unit::TestCase
  def test_timer
    b = TemporalBudTest.new
    q = Queue.new
    b.register_callback(:tik) do |t|
      assert_equal(1, t.length)
      tup = t.to_a.first
      assert(tup.val < Time.now)
      q.push(tup)
    end
    b.run_bg

    r = []
    5.times do
      r << q.pop
    end
    b.stop_bg
  end
end
