require 'test_common'

unless defined? Bud::HAVE_ZOOKEEPER
  puts "Skipping Zk test: no zookeeper Gem installed"
  return
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
  return
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
    z.delete(:path => "/foo/xyz")
    z.delete(:path => "/foo")
    z.close
  end

  def test_one_mirror
    b = ZkMirror.new
    b.run_bg
    b.sync_do {
      assert_equal([], b.t1.to_a.sort)
    }
    b.sync_do {
      b.t1 <~ [["xyz", "zzz"]]
    }
    b.sync_do {
      assert_equal([["xyz", "zzz"]], b.t1.to_a.sort)
    }
    b.stop_bg
  end
end
