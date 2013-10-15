require './test_common'

class RangeCollection
  include Bud

  state do
    range :foo, [:addr, :$id]
    range :bar, [:addr, :$id]
    range :baz, [:$id]
  end

  bootstrap do
    foo <= [["xyz", 1], ["xyz", 2], ["xyz", 3]]
  end

  bloom do
    bar <= foo {|f| [f.addr, f.id + 1]}
    baz <= (foo * bar).pairs {|f,b| [f.id + (b.id * 5)]}
  end
end

class RangeDeleteError
  include Bud

  state do
    range :foo, [:addr, :$id]
    table :bar
  end

  bloom do
    foo <- bar
  end
end

class MultipleRangeError
  include Bud

  state do
    range :foo, [:addr, :$id1, :$id2]
  end
end

class MissingRangeError
  include Bud

  state do
    range :foo, [:addr, :id]
  end
end

class TestRangeCollection < MiniTest::Unit::TestCase
  def test_basic
    r = RangeCollection.new
    assert_equal(0, r.foo.length)
    assert_equal(0, r.foo.physical_size)

    r.tick
    assert(!r.foo.empty?)
    assert_equal([["xyz", 1], ["xyz", 2], ["xyz", 3]].to_set,
                 r.foo.to_set)
    assert_equal([["xyz", 2], ["xyz", 3], ["xyz", 4]].to_set,
                 r.bar.to_set)
    assert_equal([[11], [16], [21], [12], [17], [22], [13], [18], [23]].to_set,
                 r.baz.to_set)
    assert_equal(3, r.foo.length)
    assert_equal(3, r.bar.length)
    assert_equal(9, r.baz.length)
    assert_equal(1, r.foo.physical_size)
    assert_equal(1, r.bar.physical_size)
    assert_equal(3, r.baz.physical_size)
  end

  def test_delete_error
    r = RangeDeleteError.new
    assert_raises(Bud::CompileError) { r.tick }
  end

  def test_multiple_range_error
    assert_raises(Bud::CompileError) { MultipleRangeError.new }
  end

  def test_missing_range_error
    assert_raises(Bud::CompileError) { MissingRangeError.new }
  end
end

class TestMultiRange < MiniTest::Unit::TestCase
  def test_random_dense
    digits = 0.upto(100).to_a
    100.times do
      input = digits.shuffle
      mr = Bud::MultiRange.new(input.shift)
      input.each {|i| mr << i}
      assert_equal(digits, mr.to_a)
      assert_equal(1, mr.nbuckets)
    end
  end

  def test_random_sparse
    digits = 0.upto(100).select {|d| d % 2 == 1}
    100.times do
      input = digits.shuffle
      mr = Bud::MultiRange.new(input.shift)
      input.each {|i| mr << i}
      assert_equal(digits, mr.to_a)
      assert_equal(digits.size, mr.nbuckets)
    end
  end
end
