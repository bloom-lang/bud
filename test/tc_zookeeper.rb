require 'test_common'

unless defined? Bud::HAVE_ZOOKEEPER
  puts "Skipping Zk test: no zookeeper Gem installed"
  raise
end

# Check whether ZK is running
def zk_running?
  begin
    z = Zookeeper.new("localhost:2181")
    z.close
    return true
  rescue Exception
    return false
  end
end

unless zk_running?
  puts "Skipping Zk test: cannot connect to Zookeeper on localhost:2181"
  raise
end

class ZkMirror
  include Bud

  state do
    zktable :t1, "/foo"
  end
end

class TestZk < Test::Unit::TestCase
  def setup
    z = Zookeeper.new("localhost:2181")
    zk_rm_r(z, "/foo")
    z.close
  end

  def zk_rm_r(z, root)
    r = z.get_children(:path => root)
    return unless r[:stat].exists
    r[:children].each do |c|
      zk_rm_r(z, "#{root}/#{c}")
    end
    z.delete(:path => root)
  end

  def test_one_zk
    b = ZkMirror.new
    b.run_bg
    b.sync_do {
      assert_equal([], b.t1.to_a.sort)
    }

    tuples = [["xyz", "zzz"]]
    b.sync_callback(:t1, tuples, :t1)

    b.sync_do {
      assert_equal(tuples.sort, b.t1.to_a.sort)
    }
    b.stop_bg
  end

  def test_mirror
    b1, b2 = ZkMirror.new, ZkMirror.new
    b1.run_bg
    b2.run_bg

    tuples = [["k1", "ggg"], ["k2", "ggg"]]
    q = Queue.new
    c = b2.register_callback(:t1) do |t|
      if t.length == tuples.length
        q.push(true)
      end
    end

    b1.sync_do {
      b1.t1 <~ tuples
    }

    q.pop
    b2.unregister_callback(c)

    b2.sync_do {
      assert_equal(tuples.sort, b2.t1.to_a.sort)
    }

    b1.stop_bg
    b2.stop_bg
  end
end
