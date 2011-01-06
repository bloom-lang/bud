require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'

require 'lib/kvs'
#require 'lib/kvs_metered'


module KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      #print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.members << [h] }
    end
  end

  def workload1(v)
    v.kvput <+ [["localhost:54321", "foo", 1, "bar"]]
    advance(v)
    advance(v)
    v.kvput <+ [["localhost:54321", "foo", 2, "baz"]]
    advance(v)

    v.kvput <+ [["localhost:54321", "foo", 3, "bam"]]
    advance(v)
    v.kvput <+ [["localhost:54321", "foo", 4, "bak"]]
    advance(v)
  end

  def workload2(v)
    v.kvput <+ [["localhost:54321", "foo", 1, "bar"]]
    v.kvput <+ [["localhost:54321", "foo", 2, "baz"]]
    v.kvput <+ [["localhost:54321", "foo", 3, "bam"]]
    v.kvput <+ [["localhost:54321", "foo", 4, "bak"]]
    advance(v)
  end

  def append(prog, item)
    curr = prog.bigtable.first[1]
    new = curr.clone
    new.push(item)
    #send_channel(prog.ip, prog.port, "kvput", ["#{prog.ip}:#{prog.port}", "localhost:54321", "foo", @id, new])
    prog.kvput <+ [[ "localhost:54321", "foo", @id, new ]] 
    @id = @id + 1
    soft_tick(prog)
  end

  def workload3(v)
    #send_channel(v.ip, v.port, "kvput", ["localhost:54321", "foo", 1, ["bar"]])
    v.kvput <+ [[ "localhost:54321", "foo", 1, ["bar"] ]]
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
    kvput <+ [[ "localhost:54321", "foo", 2, Array.new(curr).push("baz") ]]
    #send_channel(v.ip, v.port, "kvput", ["localhost:54321", "foo", 2, Array.new(curr).push("baz")])
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

