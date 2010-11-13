require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'
require 'test/kvs_workloads'

require 'lib/multicast'
require 'lib/kvs'
require 'lib/kvs_metered'


class TKV < Bud
  include BudKVS  
  # not necessary except to import 'tickler'
  include BestEffortMulticast
end

class RKV < Bud
  include ReplicatedKVS
  include BestEffortMulticast  
end

class TestKVS < TestLib
  include KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.members << [h] }
    end
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

  def ntest_wl5
    # the unmetered kvs fails on a disorderly workload
    v = TKV.new("localhost", 12352)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12352")
    workload2(v)
    soft_tick(v)

  
    assert_raise(RuntimeError)  { advancer(v.ip, v.port) }
  end


  def test_wl1
    # in a distributed, ordered workload, the right thing happens
    v = RKV.new("localhost", 12345, {'dump' => true})
    v2 = RKV.new("localhost", 12346)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    assert_nothing_raised(RuntimeError) {v2.run_bg}
    add_members(v, "localhost:12345", "localhost:12346")
    add_members(v2, "localhost:12345", "localhost:12346")
    sleep 1

    workload1(v)

    advance(v2)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])

    assert_equal(1, v2.bigtable.length)
  end

  def test_simple
    v = TKV.new("localhost", 12360, {'dump' => true})
    assert_nothing_raised(RuntimeError) {v.run_bg}
    #add_members(v, "localhost:12360")
    sleep 1 
  
    workload1(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end
  
end

