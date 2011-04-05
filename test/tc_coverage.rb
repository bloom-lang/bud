# gratuitous tests to ensure code coverage
require 'test_common'

class Nada
  include Bud

  state do
    table :num, [:num]
  end

  bloom do
    num <= (1..5).map{|i| [i]}
  end
end

# lib/bud.rb
class GratuitousBud < Test::Unit::TestCase
  def test_sigint
    pid = fork do 
      # XXX: There must be a better way to do this
      p = Nada.new
      Thread.new {
        p.run_bg
      }
    end
    sleep 1
    Process.kill("INT", pid)
    Process.wait
  end
  def test_sigtrap
    pid = fork do 
      # XXX: There must be a better way to do this
      p = Nada.new
      Thread.new {
        p.run_bg
      }
    end
    sleep 1
    Process.kill("TRAP", pid)
    Process.wait
  end
end

class ShootGarbage < Test::Unit::TestCase
  class SimpleChannel
    include Bud
    state {channel :c1}
  end
  
  def test_shoot_garbage
    p = SimpleChannel.new(:port => 54321)
    t = Thread.new do
      assert_raise(Bud::BudError) {p.run_fg}
    end
    socket = EventMachine::open_datagram_socket("127.0.0.1", 0)
    socket.send_datagram(1234, "127.0.0.1", 54321)
    sleep 1
    t.kill
  end
  
  def test_shoot_badtup
    p = SimpleChannel.new(:port => 54321)
    t = Thread.new do
      assert_raise(Bud::BudError) {p.run_fg}
    end
    socket = EventMachine::open_datagram_socket("127.0.0.1", 0)
    socket.send_datagram([:c1, 1234, 56].to_msgpack, "127.0.0.1", 54321)
    sleep 1
    t.kill
  end
end
