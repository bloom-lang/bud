require 'test_common'
require 'timeout'

class Vacuous
  include Bud
end

class Hooverous
  include Bud
  state {table :gotsignal, [:key]}
  bloom do
    gotsignal <= signals
    halt <= signals{[:kill]}
  end
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
    c = Vacuous.new
    c.run_bg
    c.stop(true)
    assert_equal(false, EventMachine::reactor_running?)
  end

  def test_term
    kill_with_signal("TERM")
    kill_with_signal("TERM")
  end

  def test_int
    kill_with_signal("INT")
    kill_with_signal("INT")
    bloom_signal("INT")
  end

  def kill_with_signal(sig)
    c = Vacuous.new
    cnt = 0
    q = Queue.new
    c.on_shutdown do
      cnt += 1
      q.push(true)
    end
    c.run_bg
    Process.kill(sig, $$)
    q.pop
    assert_equal(1, cnt)

    # XXX: hack. There currently isn't a convenient way to block until the kill
    # signal has been completely handled (on_shutdown callbacks are invoked
    # before the end of the Bud shutdown process). Since we don't want to run
    # another test until EM has shutdown, we can at least wait for that.
    EventMachine::reactor_thread.join
  end

  def bloom_signal(sig)
    c = Hooverous.new(:signal_handling => :bloom)
    cnt = 0
    q = Queue.new
    c.on_shutdown do
      cnt += 1
      q.push(c.gotsignal.first.key)
    end
    c.run_bg
    Process.kill(sig, $$)
    gotsig = q.pop
    assert_equal(1, cnt)
    assert_equal(sig, gotsig)

    # XXX: hack. There currently isn't a convenient way to block until the kill
    # signal has been completely handled (on_shutdown callbacks are invoked
    # before the end of the Bud shutdown process). Since we don't want to run
    # another test until EM has shutdown, we can at least wait for that.
    EventMachine::reactor_thread.join
  end

  def test_sigint_child
    kill_child_with_signal(Vacuous, "INT")
    kill_child_with_signal(Hooverous, "INT")
  end

  def test_sigterm_child
    kill_child_with_signal(Vacuous, "TERM")
    kill_child_with_signal(Hooverous, "TERM")
  end

  def kill_child_with_signal(parent_class, signal)
    parent = parent_class.new
    parent.run_bg
    pid = Bud.do_fork do
      p = Vacuous.new
      p.run_fg
    end
    sleep 1
    Process.kill(signal, pid)
    _, status = Process.waitpid2(pid)
    assert_equal(0, status)
    parent.stop
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
    c1.stop
    c2.stop
    t.join
    assert_equal(1, cnt)
  end

  class AckOnBoot
    include Bud

    bootstrap do
      @ack_io.puts ip_port
      @ack_io.puts "ready"
      on_shutdown do
        @ack_io.puts "done"
      end
    end
  end

  def test_fg_crash_shutdown_cb
    read, write = IO.pipe

    child_pid = Bud.do_fork do
      out_buf = StringIO.new
      $stdout = out_buf
      x = AckOnBoot.new
      x.instance_variable_set('@ack_io', write)
      x.run_fg
    end

    child_ip_port = read.readline.rstrip
    child_ip, child_port = child_ip_port.split(":")
    result = read.readline.rstrip
    assert_equal("ready", result)

    # Shoot garbage at the Bud instance in the child process, which should cause
    # it to shutdown
    socket = EventMachine::open_datagram_socket("127.0.0.1", 0)
    socket.send_datagram(1234, child_ip, child_port)

    Timeout::timeout(15) do
      result = read.readline.rstrip
      assert_equal("done", result)
    end
    read.close ; write.close
    Process.waitpid(child_pid)
  end

  def test_interrogate1
    c = Vacuous.new
    assert_raise(Bud::Error) {c.int_ip_port}
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
      assert_nothing_raised { c.stop }
    end
  end

  def test_extra_startage
    c = Vacuous.new
    c.run_bg
    5.times do
      assert_raise(Bud::Error) { c.run_bg }
    end
    c.stop
  end

  def test_stop_no_start
    c = Vacuous.new
    assert_nothing_raised { c.stop }
  end
end

class ThreePhase
  include Bud

  state do
    loopback :c1
    scratch :s1, [] => [:v]
    loopback :c2

    scratch :c1_done, [] => [:v]
    scratch :s1_done, [] => [:v]
    scratch :c2_done, [] => [:v]
  end

  bootstrap do
    c1 <~ [["foo", 1]]
  end

  bloom :p1 do
    c1 <~ c1 {|t| [t.key, t.val + 1] if t.val < 50}
    c1_done <= c1 {|t| [t.val] if t.val >= 50}
  end

  bloom :p2 do
    s1 <+ s1 {|s| [s.v + 1] if s.v < 3}
    s1_done <= s1 {|s| [s.v] if s.v >= 3}
  end

  bloom :p3 do
    c2 <~ s1_done {|t| ["foo", 0]}
    c2 <~ c2 {|t| [t.key, t.val + 1] if t.val < 17}
    c2_done <= c2 {|t| [t.val] if t.val >= 17}
  end
end

class TestPause < Test::Unit::TestCase
  def test_pause_threephase
    b = ThreePhase.new
    q = Queue.new
    cb_id = b.register_callback(:c1_done) do |t|
      q.push(t.to_a)
    end
    b.run_bg
    rv = q.pop
    assert_equal([50], rv.flatten)
    b.pause
    b.unregister_callback(cb_id)

    b.s1 <+ [[1]]
    b.tick
    assert_equal([[1]], b.s1.to_a)
    assert(b.s1_done.empty?)

    b.tick
    assert_equal([[2]], b.s1.to_a)
    assert(b.s1_done.empty?)

    b.tick
    assert_equal([[3]], b.s1.to_a)
    assert_equal([[3]], b.s1_done.to_a)

    b.register_callback(:c2_done) do |t|
      q.push(t.to_a)
    end
    b.run_bg
    b.sync_do # might need to force another tick
    rv = q.pop
    assert_equal([17], rv.flatten)

    b.stop
  end
end
