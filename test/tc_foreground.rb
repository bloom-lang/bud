require 'test_common'
require 'timeout'

class Vacuous
  include Bud
end

class CallbackTest < Test::Unit::TestCase
  def test_foreground
    c = Vacuous.new
    assert_raise(Timeout::Error) do
      Timeout::timeout(0.1) do
        c.run_fg
      end
    end
  end

  def test_shutdown_em
    # similarly, this test must be run early, because it blocks if any eventmachines
    # are left running by other tests (which seems to be the case)
    c = Vacuous.new
    c.run_bg
    c.stop_bg(true)
    assert_equal(false, EventMachine::reactor_running?)
  end

  def test_term
    kill_with_signal("TERM")
  end

  def test_int
    kill_with_signal("INT")
  end

  def kill_with_signal(sig)
    c = Vacuous.new
    c.run_bg
    Process.kill(sig, $$)
  end

  def test_fg_bg_mix
    c1 = Vacuous.new
    c2 = Vacuous.new
    c1.run_bg
    cnt = 0
    t = Thread.new {
      c2.run_fg
      cnt += 1
    }
    c1.stop_bg
    c2.stop_bg
    t.join
    assert_equal(1, cnt)
  end

  def test_interrogate1
    c = Vacuous.new
    assert_raise(Bud::BudError) {c.int_ip_port}
  end

  def test_interrogate2
    c = Vacuous.new
    c.run_bg
    assert_nothing_raised {c.int_ip_port}
  end

  def test_extra_stoppage
    c = Vacuous.new
    c.run_bg
    5.times do
      assert_nothing_raised { c.stop_bg }
    end
  end
end
