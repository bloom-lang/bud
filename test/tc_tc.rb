require 'rubygems'
require 'bud'
require 'test/unit'

class TcTest < Bud
  def state
    tctable :t1, ['k1', 'k2'], ['v1', 'v2']
    table :in_buf, ['k1', 'k2', 'v1', 'v2']
    table :del_buf, ['k1', 'k2', 'v1', 'v2']
  end

  declare
  def logic
    t1 <= in_buf
    t1 <- del_buf
  end
end

class TestTc < Test::Unit::TestCase
  def test_basic_ins
    t = TcTest.new('localhost', 12345)
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['1', '3', '3', '4']
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(2, t.t1.length)
  end

  def test_key_conflict_delta
    t = TcTest.new('localhost', 12346)
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['1', '2', '3', '5']
    assert_raise(Bud::KeyConstraintError) {t.tick}
  end

  def test_key_conflict
    t = TcTest.new('localhost', 12347)
    t.in_buf << ['1', '2', '3', '4']
    assert_nothing_raised(RuntimeError) {t.tick}
    t.in_buf << ['1', '2', '3', '5']
    assert_raise(Bud::KeyConstraintError) {t.tick}
  end

  def test_key_merge
    t = TcTest.new('localhost', 12348)
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['1', '2', '3', '4']
    t.in_buf << ['5', '10', '3', '4']
    t.in_buf << ['6', '10', '3', '4']
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(3, t.t1.length)
  end

  # TODO: check that each() works
  def test_scan
  end

  # TODO: check that stopping / restarting a Bud instance persists storage
  def test_persist
  end

  def test_pending_ins
  end

  def test_basic_del
    t = TcTest.new('localhost', 12349)
    t.t1 << ['1', '2', '3', '4']
    t.t1 << ['1', '3', '3', '4']
    t.t1 << ['2', '4', '3', '4']
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(3, t.t1.length)

    t.del_buf << ['2', '4', '3', '4'] # should delete
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(3, t.t1.length)
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(2, t.t1.length)

    # XXX: fix this behavior
    if false
    t.del_buf << ['1', '2', '3', '5'] # shouldn't delete
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(2, t.t1.length)
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(2, t.t1.length)
    end

    t.del_buf << ['1', '3', '3', '4'] # should delete
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(2, t.t1.length)
    assert_nothing_raised(RuntimeError) {t.tick}
    assert_equal(1, t.t1.length)
  end
end
