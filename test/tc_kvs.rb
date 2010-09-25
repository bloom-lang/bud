require 'rubygems'
require 'bud'
require 'test/unit'

require 'cidr/kvs'

class TestKVS < Test::Unit::TestCase
  def advancer(ip, port)
    sleep 1
    send_channel(ip, port, "tickler", ["#{ip}:#{port}"])
  end

  def send_channel(ip, port, chan, payload)
    EventMachine::connect(ip, port) do |c|
      pl = ([chan, payload]).to_msgpack
      c.send_data(pl)
    end
  end

  def soft_tick(v)
    assert_nothing_raised(RuntimeError) {advancer("127.0.0.1", 10001)}
    sleep 1
  end

  def test_le
    
    v = BudKVS.new("127.0.0.1", 10001)
    v.tick

    assert_nothing_raised(RuntimeError) {v.run_bg}
    sleep 1

    send_channel("127.0.0.1", 10001, "kvstore", ["127.0.0.1:10001", "127.0.0.1:10002", "foo", "bar"])
    send_channel("127.0.0.1", 10001, "kvstore", ["127.0.0.1:10001", "127.0.0.1:10002", "foo", "baz"])
    send_channel("127.0.0.1", 10001, "kvstore", ["127.0.0.1:10001", "127.0.0.1:10002", "foo", "bam"])
    send_channel("127.0.0.1", 10001, "kvstore", ["127.0.0.1:10001", "127.0.0.1:10002", "foo", "bak"])
  
    (0..2).each do |i|
      soft_tick(v)
    end 

    
    #send_channel("127.0.0.1", 10001, "kvfetch", ["127.0.0.1:10001", "127.0.0.1:10002", "foo"])
    #(0..2).each do |i|
    #  soft_tick(v)
    #end 

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end
  
end

