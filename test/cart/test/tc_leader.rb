require 'rubygems'
require 'bud'
require 'test/unit'
require 'test/test_lib'

require 'lib/paxos/leader'

class TestLE < TestLib

  def test_le
    v = LeaderElection.new("127.0.0.1", 10001, 1)
    v.tick
    assert_nothing_raised(RuntimeError) {v.run_bg}
    v.member << ['127.0.0.1:10001', 1]
    v.current_state << ['election', "127.0.0.1:10001", 0] 
  
    (0..2).each do |i|
      soft_tick(v)
    end 

    assert_equal(1, v.current_state.length)
    v.current_state.each do |c|
      assert_equal("leader", c.status)
    end
  end
  
end

