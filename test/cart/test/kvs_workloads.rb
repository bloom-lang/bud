require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'

require 'lib/kvs'
require 'lib/kvs_metered'


module KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.members << [h] }
    end
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

  def append(prog, item)
    curr = prog.bigtable.first[1]
    new = curr.clone
    new.push(item)
    send_channel(prog.ip, prog.port, "kvstore", ["#{prog.ip}:#{prog.port}", "localhost:54321", "foo", @id, new])
    @id = @id + 1
    soft_tick(prog)
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
    assert_equal(['bar','baz'], v.bigtable.first[1])
  
    @id = 3 
    append(v, "qux")
    curr = v.bigtable.first[1]
    print "CURR is now #{curr.inspect}\n"
    append(v, "baq")
    print "CURR is now #{curr.inspect}\n"
    append(v, "raz")
    print "CURR is now #{curr.inspect}\n"

  end
  
  
end

