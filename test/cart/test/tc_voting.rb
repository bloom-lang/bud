require 'lib/voting'
require 'test/unit'
require 'test/test_lib'

class TestVoting < TestLib
  def test_votingpair
    t = VotingMaster.new('localhost', 12346, {'dump' => true})
    t2 = VotingAgent.new('localhost', 12347, nil)
    t3 = VotingAgent.new('localhost', 12348, nil)
    t.run_bg
    t2.run_bg
    t3.run_bg

    t.member << ['localhost:12347']
    t.member << ['localhost:12348']

    t.begin_vote <+ [[1, 'me for king']]
    advance(t)
    advance(t)

    assert_equal([1,'me for king', 'localhost:12346'], t2.peer_ballot_cache.first)
    assert_equal([1,'me for king', 'localhost:12346'], t3.peer_ballot_cache.first)
    
    t2.cast_vote <+ [[1, "hell yes"]]
    advance(t2)
    advance(t3)

    assert_equal([1, 'hell yes', 1], t.vote_cnt.first)
    assert_equal([1, 'me for king', 'in flight'], t.vote_status.first)

    t3.cast_vote <+ [[1, "hell yes"]]
    advance(t2)
    advance(t3)

    advance(t)
    advance(t)

    assert_equal([1, 'hell yes', 2], t.vote_cnt.first)
    assert_equal([1, 'me for king', 'hell yes'], t.vote_status.first)
  end
    
end
