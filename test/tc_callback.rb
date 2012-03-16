require './test_common'
require 'timeout'

class SimpleCb
  include Bud

  state do
    scratch :t1
    scratch :c1
  end

  bloom do
    c1 <= t1
  end
end

class CallbackAtNext
  include Bud

  state do
    scratch :t1
    scratch :c1
  end

  bloom do
    c1 <+ t1
  end
end

class TickingCallback
  include Bud

  state do
    periodic :tic, 0.1
    scratch :dummy
  end

  bloom do
    dummy <= tic
  end
end

class CallbackWithChannel
  include Bud

  state do
    channel :cin
    scratch :iout
  end
    
  bloom do
    iout <= cin
  end
end

class CallbackTest < MiniTest::Unit::TestCase
  class Counter
    attr_reader :cnt

    def initialize
      @cnt = 0
    end

    def bump
      @cnt += 1
    end
  end

  def test_simple_cb
    c = SimpleCb.new
    call_tick = Counter.new
    tuple_tick = Counter.new
    c.register_callback(:c1) do |t|
      call_tick.bump
      t.length.times do
        tuple_tick.bump
      end
    end

    c.run_bg
    c.sync_do
    assert_equal(0, call_tick.cnt)
    assert_equal(0, tuple_tick.cnt)
    c.sync_do {
      c.t1 <+ [[5, 10]]
    }
    assert_equal(1, call_tick.cnt)
    assert_equal(1, tuple_tick.cnt)
    c.sync_do {
      c.t1 <+ [[10, 15], [20, 25]]
    }
    assert_equal(2, call_tick.cnt)
    assert_equal(3, tuple_tick.cnt)
    c.stop
  end

  def test_cb_at_next
    c = CallbackAtNext.new
    c.run_bg
    tick = Counter.new
    c.register_callback(:c1) do |t|
      tick.bump
    end

    c.sync_do {
      c.t1 <+ [[20, 30]]
    }
    assert_equal(0, tick.cnt)
    c.sync_do
    assert_equal(1, tick.cnt)

    c.stop
  end

  def test_missing_cb_error
    c = SimpleCb.new
    assert_raises(Bud::Error) do
      c.register_callback(:crazy) do
        raise RuntimeError
      end
    end
  end

  def test_blocking_on_callback
    c = SimpleCb.new
    c.run_bg
    tuples = [[1, 2]]
    c.sync_callback(:t1, tuples, :c1) do |cb|
      assert_equal(1, cb.length)
    end
    c.stop
  end

  def test_delta
    c = TickingCallback.new
    c.run_bg
    Timeout::timeout(5) {c.delta(:tic)}
    c.stop
  end

  def add_cb(b)
    tick = Counter.new
    id = b.register_callback(:c1) do
      tick.bump
    end
    return [tick, id]
  end

  def test_unregister_cb
    c = SimpleCb.new
    tick1, id1 = add_cb(c)
    tick2, id2 = add_cb(c)

    c.run_bg
    c.sync_do {
      c.t1 <+ [[100, 200]]
    }
    assert_equal(1, tick1.cnt)
    assert_equal(1, tick2.cnt)
    c.unregister_callback(id1)
    c.sync_do {
      c.t1 <+ [[200, 400]]
    }
    assert_equal(1, tick1.cnt)
    assert_equal(2, tick2.cnt)
    c.stop
  end

  def test_callback_with_channel
    c = CallbackWithChannel.new
    c.run_bg
    c.sync_callback(:cin, [[c.ip_port, "foo"]], :iout)
    assert(true)
  end

  def test_shutdown_cb
    cnt = 0
    c = TickingCallback.new
    c.on_shutdown do
      cnt += 1
    end
    c.run_bg
    assert_equal(0, cnt)
    c.stop
    assert_equal(1, cnt)
  end
end
