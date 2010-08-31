require 'rubygems'
require 'bud'
require 'test/unit'

require 'examples/leader'
class TestLE < Test::Unit::TestCase
  def soft_tick(v)
    assert_nothing_raised(RuntimeError) {v.tickle}
    sleep 1
  end

  def test_le
    v = LeaderElection.new("127.0.0.1", 10001, 1)

    assert_nothing_raised(RuntimeError) {v.run_bg}
    v.member << ['127.0.0.1:10001', 1]
  
    (0..2).each do |i|
      soft_tick(v)
    end 

    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      assert_equal("leader", c.status)
    end
  end
  
  def ntest_fail_le
    v = LeaderElection.new("127.0.0.1", 10003, 1)
    assert_nothing_raised(RuntimeError) {v.run_bg}
    v.member << ['127.0.0.1:10002', 1]
    v.member << ['127.0.0.1:10002', 1]
  
    (0..2).each do |i|
      soft_tick(v)
    end
    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      assert_equal("election", c.status)
    end
  end
end

