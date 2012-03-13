require './test_common'

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

class TestTimer < MiniTest::Unit::TestCase
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

    5.times { q.pop }
    b.stop
  end

  def test_timer_tick
    b = TemporalBudTest.new
    tick_cnt = 0
    b.register_callback(:tik) do |t|
      tick_cnt += 1
    end

    b.tick
    sleep 0.4
    b.tick
    assert_equal(1, tick_cnt)
    sleep 0.4
    b.tick
    assert_equal(2, tick_cnt)
    b.stop
  end
end

class BudClockExample
  include Bud

  state do
    table :t1
    scratch :in_tbl, [:val]
    scratch :in_tbl2, [:val]
  end

  bloom do
    t1 <= in_tbl {|t| [t.val, bud_clock]}
    t1 <= in_tbl2 {|t| [t.val, bud_clock]}
  end
end

class TestBudClock < MiniTest::Unit::TestCase
  def test_bud_clock
    b = BudClockExample.new
    b.run_bg
    b.sync_do {
      b.in_tbl <+ [[5]]
      b.in_tbl2 <+ [[5]]
    }
    b.stop
    assert_equal(1, b.t1.length)
  end

  def test_bud_clock_outside_tick
    b = BudClockExample.new
    b.run_bg
    assert_raises(Bud::Error) do
      b.sync_do {
        puts "Current Bud clock: #{b.bud_clock}"
      }
    end
    b.stop
  end
end
