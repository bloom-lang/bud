require 'rubygems'
require 'bud'
require 'lib/queue'
require 'test/unit'

class Q < Bud
  include BaseQueue
end

class TestQueue < Test::Unit::TestCase


  def test_q
    kyu = Q.new('localhost', 10001)
    kyu.tick

    kyu.q << [1, 'foo']
    kyu.q << [3, 'bar']
    kyu.q << [26, 'baz']
    kyu.q << [100, 'qux']

    assert_equal(0, kyu.head.length)
    assert_nothing_raised(RuntimeError) { kyu.tick }
    assert_equal([1, 'foo'], kyu.head.first)
    # still there: time hasn't moved.
    assert_equal([1, 'foo'], kyu.head.first)
    assert_nothing_raised(RuntimeError) { kyu.tick }
    # but now it's gone
    assert_equal(0, kyu.head.length)

    # now we request the next value
    kyu.consumed <= [[1]]
    assert_equal(0, kyu.head.length)
    # which should be available in the next timestep,
    # but for some reason requires two!!
    assert_nothing_raised(RuntimeError) { kyu.tick }
    assert_nothing_raised(RuntimeError) { kyu.tick }
    assert_equal([3, 'bar'], kyu.head.first)

    (0..10).each do |i|
      kyu.head.each do |h| 
        print "HEAD(#{i}): #{h.inspect}\n"
        kyu.consumed <= [[h.ident]]
      end
      kyu.tick
    end
    assert_equal(0, kyu.q.length)
  end
end
