require 'lib/2pc'
require 'test/unit'
require 'test/test_lib'


class TestVoting < TestLib
  def test_singlenode
    t = TwoPCMaster.new('localhost', 12345, nil)
    t2 = TwoPCAgent.new('localhost', 12346, nil)
    t3 = TwoPCAgent.new('localhost', 12347, nil)
    t.run_bg
    t2.run_bg
    t3.run_bg
    t.member << ['localhost:12346']
    t.member << ['localhost:12347']
    t.request_commit <+ [[ 1, "foobar" ]]
    advance(t)
    advance(t)

    assert_equal(1, t.xact.length)
    assert_equal("prepare", t.xact.first[2])

    assert_equal(1, t2.peer_ballot_cache.length)

    t2.cast_vote <+ [[ 1, "Y" ]]
    advance(t2)
    advance(t)
    assert_equal(1, t.master_vote_cache.length)

    t3.cast_vote <+ [[ 1, "Y" ]]
    advance(t3)
    advance(t)

    advance(t)
    advance(t)
    assert_equal(1, t.xact.length)
    assert_equal("commit", t.xact.first[2])
  end
end
