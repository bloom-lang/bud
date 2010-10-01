require 'rubygems'
require 'bud'
require 'test/unit'

class TestLib < Test::Unit::TestCase

  def add_members(b, *hosts)
    hosts.each do |h|
      assert_nothing_raised(RuntimeError) { b.member << [h] }
    end
  end

  def advance(p)
    advancer(p.ip, p.port)
  end

  def advancer(ip, port)
    sleep 1
    send_channel(ip, port, "tickler", ["#{ip}:#{port}"])
  end

  def send_channel(ip, port, chan, payload)
    EventMachine::connect(ip, port) do |c|
      pl = ([chan, payload]).to_msgpack
      assert_nothing_raised(RuntimeError) { c.send_data(pl) }
    end
  end

  def soft_tick(v)
    assert_nothing_raised(RuntimeError) {advancer(v.ip, v.port)}
    sleep 1
  end

  def test_empty

  end
end
