require './test_common'

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

ZK_ROOT = "/foo"

class ZkMirror
  include Bud

  state do
    store :t1, :zookeeper, :path => ZK_ROOT, :addr => 'localhost:2181'
    table :dummy
    scratch :t1_is_empty
  end

  bootstrap do
    dummy << [1,1]
  end

  # XXX: This is a hack: we want t1_is_empty to have a tuple iff t1.empty? is
  # true
  bloom do
    t1_is_empty <= dummy {|t| t if t1.empty?}
  end
end

class TestZk < MiniTest::Unit::TestCase
  def setup
    zk_delete(ZK_ROOT)
  end

  def zk_delete(path)
    z = Zookeeper.new("localhost:2181")
    zk_rm_r(z, path)
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
    b.stop
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

    c = b2.register_callback(:t1) do |t|
      q.push(true) if t.length == 1
    end
    zk_delete(ZK_ROOT + "/k1")
    q.pop
    b2.unregister_callback(c)
    b2.sync_do {
      assert_equal([["k2", "ggg"]], b2.t1.to_a.sort)
    }

    c = b2.register_callback(:t1_is_empty) do |t|
      q.push(true)
    end
    zk_delete(ZK_ROOT + "/k2")
    q.pop
    b2.unregister_callback(c)

    b2.sync_do {
      assert_equal([], b2.t1.to_a.sort)
    }

    b1.stop
    b2.stop
  end

  def test_ephemeral
    b1, b2 = ZkMirror.new, ZkMirror.new
    b1.run_bg
    b2.run_bg

    b2.sync_do {
      b2.t1 <~ [["baz", "xyz"]]
    }

    q = Queue.new
    c = b2.register_callback(:t1) do |t|
      q.push(true) if t.length == 2
    end
    b1.sync_do {
      b1.t1 <~ [["foo", "bar", {:ephemeral => true}]]
    }
    q.pop
    b2.unregister_callback(c)

    b2.sync_do {
      assert_equal([["baz", "xyz"], ["foo", "bar"]], b2.t1.to_a.sort)
    }

    c = b2.register_callback(:t1) do |t|
      q.push(true) if t.length == 1
    end
    b1.stop
    q.pop
    b2.unregister_callback(c)
    b2.sync_do {
      assert_equal([["baz", "xyz"]], b2.t1.to_a.sort)
    }

    b2.stop
  end

  def test_sequence
    b = ZkMirror.new
    b.run_bg

    q = Queue.new
    c = b.register_callback(:t1) do |t|
      q.push(true) if t.length == 3
    end
    b.sync_do { b.t1 <~ [["a_", "kkk", {:sequence => true}]] }
    b.sync_do { b.t1 <~ [["b_", "kkk", {:sequence => true}]] }
    b.sync_do { b.t1 <~ [["c_", "kkk", {:sequence => true}]] }

    q.pop
    b.unregister_callback(c)

    b.sync_do {
      assert_equal([["a_0000000000", "kkk"],
                    ["b_0000000001", "kkk"],
                    ["c_0000000002", "kkk"]],
                   b.t1.to_a.sort)
    }

    b.stop
  end
end
