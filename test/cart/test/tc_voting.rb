require 'lib/voting'
require 'test/unit'
require 'test/test_lib'


class TestVoting < TestLib
  def test_singlenode
    t = Voting.new('localhost', 12345, nil)
    t.run_bg
    t.member << ['localhost:12345']

    t.begin_vote <+ [[1, 'me for king']]
    advance(t)
    advance(t)

    assert_equal([1,'me for king', 'localhost:12345'], t.peer_vote_cache.first)
    t.cast_vote <+ [[1, "hell yes"]]
    advance(t)
    advance(t)
    assert_equal([1, 'hell yes', 1], t.vote_cnt.first)

    assert_equal([1, 'me for king', 'hell yes'], t.vote_status.first)

  end


  def test_votingpair
    t = Voting.new('localhost', 12346, nil)
    t2 = Voting.new('localhost', 12347, nil)
    t.run_bg
    t2.run_bg

    t.member << ['localhost:12346']
    t.member << ['localhost:12347']

    t.begin_vote <+ [[1, 'me for king']]
    advance(t)
    advance(t)

    assert_equal([1,'me for king', 'localhost:12346'], t.peer_vote_cache.first)
    assert_equal([1,'me for king', 'localhost:12346'], t2.peer_vote_cache.first)

    
    t.cast_vote <+ [[1, "hell yes"]]
    advance(t)

    assert_equal([1, 'hell yes', 1], t.vote_cnt.first)
    assert_equal([1, 'me for king', 'in flight'], t.vote_status.first)

    t2.cast_vote <+ [[1, "hell yes"]]
    advance(t2)
    advance(t)
    advance(t)

    assert_equal([1, 'hell yes', 2], t.vote_cnt.first)
    assert_equal([1, 'me for king', 'hell yes'], t.vote_status.first)
  end
    
end
