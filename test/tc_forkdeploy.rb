require 'test_common'
require 'stringio'
require '../examples/deploy/tokenring'
require 'timeout'

NUM_DEPLOY_FORKS = 10

class RingFork
  include Bud
  include TokenRing
  include ForkDeploy

  deploystrap do
    node_count << [NUM_DEPLOY_FORKS]
  end

  bloom :pass_token_once do
    next_node <- (next_node * token_persist).lefts
  end
end

class TestForkDeploy < Test::Unit::TestCase
  def test_fork_deploy
    deploy_out = StringIO.new
    begin
      $stdout = deploy_out
      do_fork_test
    ensure
      $stdout = STDOUT
    end
  end

  def do_fork_test
    read, write = IO.pipe
    child_opts = { :stdout => write }
    ring_fork = RingFork.new(:deploy => true, :stdout => write,
                             :deploy_child_opts => child_opts)
    ring_fork.run_bg

    lines = []
    Timeout::timeout(45) do
      (NUM_DEPLOY_FORKS + 1).times do
        lines << read.readline
      end
    end

    # Close pipe
    read.close
    write.close

    # Token starts and ends up at the same place
    assert_equal(lines.first, lines.last)

    # Token circulates amongst nodes
    for i in (1..lines.size-1)
      assert(lines[i]["Got token!"])
      for j in (i+1..lines.size-1)
        assert_not_equal(lines[i], lines[j])
      end
    end

    ring_fork.stop_bg
    # Assert there are no child processes left; we've closed them all
    assert_equal(Process.waitall, [])
  end
end
