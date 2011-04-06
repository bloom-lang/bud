require 'test_common'
require 'fileutils'

class TcTest
  include Bud

  state do
    tctable :t1, [:k1, :k2] => [:v1, :v2]
    table :in_buf, [:k1, :k2, :v1, :v2]
    table :del_buf, [:k1, :k2, :v1, :v2]
    table :pending_buf, [:k1, :k2] => [:v1, :v2]
    table :pending_buf2, [:k1, :k2] => [:v1, :v2]

    scratch :t2, [:k] => [:v]
    scratch :t3, [:k] => [:v]
    scratch :t4, [:k] => [:v]
    tctable :chain_start, [:k] => [:v]
    tctable :chain_del, [:k] => [:v]

    tctable :join_t1, [:k] => [:v1, :v2]
    tctable :join_t2, [:k] => [:v1, :v2]
    scratch :cart_prod, [:k, :v1]
    scratch :join_res, [:k, :v1]
  end

  bloom do
    t1 <= in_buf
    t1 <- del_buf
    t1 <+ pending_buf
    t1 <+ pending_buf2
  end

  bloom :do_chain do
    t2 <= chain_start.map{|c| [c.k, c.v + 1]}
    t3 <= t2.map{|c| [c.k, c.v + 1]}
    t4 <= t3.map{|c| [c.k, c.v + 1]}
    chain_start <- chain_del
  end

  bloom :do_join do
    temp :k <= join([join_t1, join_t2], [join_t1.k, join_t2.k])
    join_res <= k
    cart_prod <= join([join_t1, join_t2])
  end
end

BUD_DIR = "#{Dir.pwd}/bud_tmp"

def setup_bud
  rm_bud_dir
end

def cleanup_bud(b)
  unless b.nil?
    b.close_tables
  end
  rm_bud_dir
end

def rm_bud_dir
  return unless File.directory? BUD_DIR
  FileUtils.rm_r(BUD_DIR)
end

class TestTc < Test::Unit::TestCase
  def setup
    setup_bud
    @t = make_bud(true)
  end

  def teardown
    cleanup_bud(@t)
    @t = nil
  end

  def make_bud(truncate)
    TcTest.new(:tc_dir => BUD_DIR, :tc_truncate => truncate, :quiet => true)
  end

  def test_basic_ins
    assert_equal(0, @t.t1.length)
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['1', '3', '3', '4']
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)
    assert(@t.t1.include? ['1', '2', '3', '4'])
    assert(@t.t1.has_key? ['1', '2'])
    assert_equal(false, @t.t1.include?(['1', '2', '3', '5']))
  end

  def test_key_conflict_delta
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['1', '2', '3', '5']
    assert_raise(Bud::KeyConstraintError) {@t.tick}
  end

  def test_key_conflict
    @t.in_buf << ['1', '2', '3', '4']
    assert_nothing_raised(RuntimeError) {@t.tick}
    @t.in_buf << ['1', '2', '3', '5']
    assert_raise(Bud::KeyConstraintError) {@t.tick}
  end

  def test_key_merge
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['1', '2', '3', '4']
    @t.in_buf << ['5', '10', '3', '4']
    @t.in_buf << ['6', '10', '3', '4']
    @t.in_buf << ['6', '10', '3', '4']

    @t.t1 << ['1', '2', '3', '4']
    @t.t1 << ['1', '2', '3', '4']

    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(3, @t.t1.length)
  end

  def test_persist
    @t.in_buf << [1, 2, 3, 4]
    @t.in_buf << [5, 10, 3, 4]
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)

    10.times do |i|
      @t.close_tables
      @t = make_bud(false)
      @t.in_buf << [6, 10 + i, 3, 4]
      assert_nothing_raised(RuntimeError) {@t.tick}
      assert_equal(3 + i, @t.t1.length)
    end
  end

  def test_pending_ins
    @t.pending_buf << ['1', '2', '3', '4']
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(0, @t.t1.length)
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(1, @t.t1.length)
  end

  def test_pending_key_conflict
    @t.pending_buf << ['1', '2', '3', '4']
    @t.pending_buf2 << ['1', '2', '3', '5']
    assert_raise(Bud::KeyConstraintError) {@t.tick}
  end

  def test_basic_del
    @t.t1 << ['1', '2', '3', '4']
    @t.t1 << ['1', '3', '3', '4']
    @t.t1 << ['2', '4', '3', '4']
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(3, @t.t1.length)

    @t.del_buf << ['2', '4', '3', '4'] # should delete
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(3, @t.t1.length)
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)

    @t.del_buf << ['1', '3', '3', '5'] # shouldn't delete
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)

    @t.del_buf << ['1', '3', '3', '4'] # should delete
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t1.length)
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(1, @t.t1.length)
  end

  def test_chain
    @t.chain_start << [5, 10]
    @t.chain_start << [10, 15]
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.t2.length)
    assert_equal(2, @t.t3.length)
    assert_equal(2, @t.t4.length)
    assert_equal([10,18], @t.t4[[10]])

    @t.chain_del << [5,10]
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(2, @t.chain_start.length)
    assert_equal(2, @t.t2.length)
    assert_equal(2, @t.t3.length)
    assert_equal(2, @t.t4.length)
    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(1, @t.chain_start.length)
    assert_equal(1, @t.t2.length)
    assert_equal(1, @t.t3.length)
    assert_equal(1, @t.t4.length)
  end

  def test_cartesian_product
    @t.join_t1 << [12, 50, 100]
    @t.join_t1 << [15, 50, 120]
    @t.join_t2 << [12, 70, 150]
    @t.join_t2 << [6, 20, 30]

    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(4, @t.cart_prod.length)

    @t.join_t2 << [6, 20, 30] # dup
    @t.join_t2 << [18, 70, 150]

    assert_nothing_raised(RuntimeError) {@t.tick}
    assert_equal(6, @t.cart_prod.length)
  end

  def test_join
    @t.join_t1 << [12, 50, 100]
    @t.join_t1 << [15, 50, 120]
    @t.join_t2 << [12, 70, 150]
    @t.join_t2 << [6, 20, 30]
    assert_nothing_raised(RuntimeError) {@t.tick}

    assert_equal(1, @t.join_res.length)
  end
end

class TcNest
  include Bud

  state {
    scratch :in_buf, [:k1, :k2] => [:v1]
    table :t1, [:k1] => [:v1]
    tctable :t2, [:k1, :k2] => [:v1, :v2]
  }

  bootstrap do
    t1 << [5, 10]
  end

  bloom do
    t2 <= (in_buf * t1).pairs {|b, t| [b.k1, b.k2, b.v1, t]}
  end
end

class TestNestedTc < Test::Unit::TestCase
  def setup
    setup_bud
    @t = make_bud
  end

  def teardown
    cleanup_bud(@t)
    @t = nil
  end

  def make_bud
    TcNest.new(:tc_dir => BUD_DIR, :tc_truncate => true, :quiet => true)
  end

  def test_basic_nest
    @t.run_bg

    @t.sync_do {
      @t.in_buf <+ [[10, 20, 30]]
    }
    @t.sync_do {
      # We can store nested tuples inside TC tables, but we lose the ability to
      # access named columns after deserialization.
      assert_equal([10, 20, 30, [5, 10]], @t.t2.first)
    }

    @t.stop_bg
  end
end

class TcBootstrap
  include Bud

  state do
    tctable :t1
  end

  bootstrap do
    t1 << [5, 10]
    t1 << [10,15]
  end
end

class TestTcBootstrap < Test::Unit::TestCase
  def setup
    setup_bud
    @t = make_bud
  end

  def teardown
    cleanup_bud(@t)
    @t = nil
  end

  def make_bud
    TcBootstrap.new(:tc_dir => BUD_DIR, :tc_truncate => false, :quiet => true)
  end

  def test_basic
    def check_t
      @t.run_bg
      @t.sync_do {
        assert_equal([[5, 10], [10, 15]], @t.t1.to_a.sort)
      }
      @t.stop_bg
    end

    check_t
    @t = make_bud
    check_t
  end
end
