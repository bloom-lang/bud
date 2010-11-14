require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'
require 'test/kvs_workloads'

require 'lib/kvs'
require 'lib/kvs_metered'
require 'lib/multicast'


class MKV < Bud
  include ReplicatedKVS
  include KVSMetering
  include BestEffortMulticast
end

class TestKVS < TestLib
  include KVSWorkloads

  #def add_members(b, *hosts)
  #  hosts.each do |h|
  #    print "ADD MEMBER: #{h.inspect}\n"
  #    assert_nothing_raised(RuntimeError) { b.members << [h] }
  #  end
  #end

  def ntest_metered_testandset
    v = MKV.new("localhost", 23456)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:23456")
    workload3(v)
  end

  def ntest_wl2
    # reliable delivery fails if the recipient is down
    v = TKV.new("localhost", 12347, nil) # {'visualize' => true})
    assert_nothing_raised(RuntimeError) {v.run_bg}
    sleep 1
    add_members(v, "localhost:12347", "localhost:12348")
    if v.is_a?  ReliableDelivery
      sleep 1
      workload1(v)
      assert_equal(0, v.bigtable.length)
    end
    
  end

  def test_wl3
    # the metered kvs succeeds on the naive workload
    v = MKV.new("localhost", 12350, {'dump' => true})
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12350")
    workload1(v)
    advance(v)
    advance(v)
    advance(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end


  def test_wl4
    # the metered kvs also succeeds on a disorderly workload
    v = MKV.new("localhost", 12351)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12351")
    workload2(v)

    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end
  
end

