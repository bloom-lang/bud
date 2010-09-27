require 'rubygems'
require 'bud'
require 'test/unit'
require 'test_lib'

require 'cidr/kvs'

class TestKVS < TestLib
  def setupkvs(port)
    v = BudKVS.new("localhost", port)
    v.tick
    add_members(v)
    return v
  end


  def ntest_wl2
    v = setupkvs(12345)
    if v.is_a?  ReliableDelivery
      assert_nothing_raised(RuntimeError) {v.run_bg}
      sleep 1
      workload1(v)
      assert_equal(0, v.bigtable.length)
    end
    
  end

  def test_wl1
    v = setupkvs(12345)
    v2 = setupkvs(12346)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    assert_nothing_raised(RuntimeError) {v2.run_bg}
    sleep 1

    workload1(v)
    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])

    print "uym\n"

    (0..3).each do |i|
      soft_tick(v2)
    end
    assert_equal(1, v2.bigtable.length)
  end


  def workload1(v)
    print "v is #{v.inspect}\n"
    send_channel("localhost", 12345, "kvstore", ["localhost:12345", "localhost:54321", "foo", 1, "bar"])
    send_channel("localhost", 12345, "kvstore", ["localhost:12345", "localhost:54321", "foo", 2, "baz"])
    send_channel("localhost", 12345, "kvstore", ["localhost:12345", "localhost:54321", "foo", 3, "bam"])
    send_channel("localhost", 12345, "kvstore", ["localhost:12345", "localhost:54321", "foo", 4, "bak"])
  
    (0..3).each do |i|
      soft_tick(v)
    end 
  end
  
end

