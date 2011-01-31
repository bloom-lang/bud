require 'lib/voting'
require 'test/unit'
require 'test/test_lib'

class VM < Bud
  include VotingMaster
end

class VA < Bud
  include VotingAgent
end

class TestVoting < TestLib
  def test_votingpair
    t = VM.new(:port => 12346, :dump => true, :visualize => true)
    t2 = VA.new(:port => 12347, :visualize => true)
    t3 = VA.new(:port => 12348)
    t.run_bg
    t2.run_bg
    t3.run_bg

    t.member << ['localhost:12347']
    t.member << ['localhost:12348']

    t.begin_vote <+ [[1, 'me for king']]
    advance(t)
    advance(t)

    assert_equal([1,'me for king', 'localhost:12346'], t2.waiting_ballots.first)
    assert_equal([1,'me for king', 'localhost:12346'], t3.waiting_ballots.first)

    #t2.cast_vote <+ [[1, "hell yes"]]
    advance(t2)
    advance(t3)

    #assert_equal([1, 'hell yes', 1], t.vote_cnt.first)
    #assert_equal([1, 'me for king', 'in flight'], t.vote_status.first)

    #t3.cast_vote <+ [[1, "hell yes"]]
    advance(t2)
    advance(t3)

    #advance(t)
    #advance(t)

    assert_equal([1, 'yes', 2], t.vote_cnt.first)
    assert_equal([1, 'me for king', 'yes'], t.vote_status.first)
  end
end
