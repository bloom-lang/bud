require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'

require 'lib/kvs'
require 'lib/kvs_metered'

class TestKVS < TestLib

  def add_members(b, *hosts)
    hosts.each do |h|
      print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.member << [h] }
    end
  end

  def ntest_metered_testandset
    v = MeteredKVS.new("localhost", 23456)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:23456")
    workload3(v)
  end

  def test_wl2
    # reliable delivery fails if the recipient is down
    v = BudKVS.new("localhost", 12347, nil) # {'visualize' => true})
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
    v = MeteredKVS.new("localhost", 12350)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12350")
    workload1(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end


  def test_wl4
    # the metered kvs also succeeds on a disorderly workload
    v = MeteredKVS.new("localhost", 12351)
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


  def test_wl5
    # the unmetered kvs fails on a disorderly workload
    v = BudKVS.new("localhost", 12352)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12352")
    workload2(v)
    soft_tick(v)

  
    assert_raise(RuntimeError)  { advancer(v.ip, v.port) }
  end


  def test_wl1
    # in a distributed workload, the right thing happens
    v = BudKVS.new("localhost", 12345)
    v2 = BudKVS.new("localhost", 12346)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    assert_nothing_raised(RuntimeError) {v2.run_bg}
    add_members(v, "localhost:12345", "localhost:12346")
    add_members(v2, "localhost:12345", "localhost:12346")
    sleep 1

    workload1(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])

    assert_equal(1, v2.bigtable.length)
  end

  def test_simple
    v = BudKVS.new("localhost", 12360)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12360")
    sleep 1 
  
    workload1(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end


  def workload1(v)
    # note that this naive key-value store will throw an error if we try to insert
    # two conflicting keys in the same timestep.  below, we ensure that we control
    # the order in which they appear.
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 1, "bar"])
    soft_tick(v)
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 2, "baz"])
    soft_tick(v)
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 3, "bam"])
    soft_tick(v)
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 4, "bak"])
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
  end


  def workload2(v)
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 1, "bar"])
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 2, "baz"])
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 3, "bam"])
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 4, "bak"])
    #soft_tick(v)
    #soft_tick(v)
    #soft_tick(v)
  end

  def workload3(v)
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 1, ["bar"]])
  
    print "STORE\n"
    soft_tick(v)
    print "TICKED one\n"
    soft_tick(v)
    print "AHEM\n"
    assert_equal(1, v.bigtable.length)
    assert_equal("foo", v.bigtable.first[0])
    curr = v.bigtable.first[1]

    print "OK!\n"
    #print "curr is #{curr.inspect}\n"
    
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 2, Array.new(curr).push("baz")])
    soft_tick(v)
    soft_tick(v)

    assert_equal("foo", v.bigtable.first[0])
    curr = v.bigtable.first[1]
    assert_equal(['bar','baz'], curr)
   
    print "curr is #{curr.join(',')}\n" 
    send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 2, Array.new(curr).push("qux")])
    #send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 2, curr.push("qux")])
    #curr = v.bigtable.first[1]
    ##send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 3, Array.new(curr).push("boom")])
    #curr = v.bigtable.first[1]
    ##send_channel(v.ip, v.port, "kvstore", ["#{v.ip}:#{v.port}", "localhost:54321", "foo", 4, Array.new(curr).push("bif")])

    print "curr is #{curr.join(',')}\n" 
    

    curr = v.bigtable.first[1]
    print "CURR is now #{curr.inspect}\n"
    soft_tick(v)
    print "CURR is now #{curr.inspect}\n"
    soft_tick(v)
    print "CURR is now #{curr.inspect}\n"
    

  end
  
  
end

