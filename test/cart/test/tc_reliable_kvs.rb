require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'
require 'test/kvs_workloads'

require 'lib/kvs'
require 'lib/kvs_metered'
require 'lib/multicast'


class RMKV < Bud
  include ReplicatedKVS
  include KVSMetering
  include ReliableMulticast
end

class TestKVS < TestLib
  include KVSWorkloads

  def add_members(b, *hosts)
    hosts.each do |h|
      print "ADD MEMBER: #{h.inspect}\n"
      assert_nothing_raised(RuntimeError) { b.members << [h] }
    end
  end

  def test_wl2
    # reliable delivery fails if the recipient is down
    v = RMKV.new(:port => 12347)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    sleep 1
    add_members(v, "localhost:12347", "localhost:12348")
    if v.is_a?  ReliableDelivery
      sleep 1
      workload1(v)
      assert_equal(0, v.bigtable.length)
    end
  end
end

