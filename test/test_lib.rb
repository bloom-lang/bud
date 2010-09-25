require 'rubygems'
require 'bud'
require 'test/unit'

class TestLib < Test::Unit::TestCase
  def advance(p)
    sleep 1
    send_channel(p, "tickler", ["#{p.ip}:#{p.port}"])
  end

  def send_channel(c, chan, payload)
    EventMachine::connect(c.ip, c.port) do |c|
      pl = ([chan, payload]).to_msgpack
      c.send_data(pl)
    end
  end

  def test_empty

  end
end
