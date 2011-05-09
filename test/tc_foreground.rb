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
    c = Vacuous.new
    c.run_bg
    c.stop_bg(true)
    assert_equal(false, EventMachine::reactor_running?)
  end

  def test_term
    kill_with_signal("TERM")
    kill_with_signal("TERM")
  end

  def test_int
    kill_with_signal("INT")
    kill_with_signal("INT")
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

  def test_sigint_child
    kill_child_with_signal("INT")
  end

  def test_sigterm_child
    kill_child_with_signal("TERM")
  end

  def kill_child_with_signal(signal)
    parent = Vacuous.new
    parent.run_bg
    pid = Bud.do_fork do
      p = Vacuous.new
      p.run_fg
    end
    sleep 1
    Process.kill(signal, pid)
    _, status = Process.waitpid2(pid)
    assert_equal(0, status)
    parent.stop_bg
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

  class AckOnBoot
    include Bud

    bootstrap do
      @ack_io.puts ip_port
      @ack_io.puts "ready"
    end
  end

  def test_fg_crash_shutdown_cb
    read, write = IO.pipe

    child_pid = Bud.do_fork do
      out_buf = StringIO.new
      $stdout = out_buf
      x = AckOnBoot.new
      x.instance_variable_set('@ack_io', write)
      x.on_shutdown do
        sleep 2
        write.puts "done"
      end
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

    result = read.readline.rstrip
    assert_equal("done", result)
    read.close ; write.close
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
