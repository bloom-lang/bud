require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'
require 'test/kvs_workloads'

require 'lib/useful_combos'


class TestKVS < TestLib
  include KVSWorkloads

  def test_metered_testandset
    v = MeteredBestEffortReplicatedKVS.new("localhost", 23456)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:23456")
    workload3(v)
  end

  def ntest_wl3
    # the metered kvs succeeds on the naive workload
    v = MeteredBestEffortReplicatedKVS.new("localhost", 13350, {'dump' => true})
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12350")
    workload1(v)
    advance(v)
    advance(v)
    advance(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end


  def ntest_wl4
    # the metered kvs also succeeds on a disorderly workload
    v = MeteredBestEffortReplicatedKVS.new("localhost", 13351)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    add_members(v, "localhost:12351")
    workload2(v)

    soft_tick(v)
    soft_tick(v)
    soft_tick(v)
    soft_tick(v)

    assert_equal(1, v.bigtable.length)
    assert_equal("bak", v.bigtable.first[1])
  end
  
end

