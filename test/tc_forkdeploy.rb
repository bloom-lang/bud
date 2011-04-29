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
    next_node <- (next_node * token_persist).lefts {|n| n.node}
  end
end

class TestForkDeploy < Test::Unit::TestCase
  def test_fork_deploy
    read, write = IO.pipe
    $stdout = write
    ring_fork = RingFork.new(:deploy => true)
    ring_fork.run_bg

    lines = []
    begin
      Timeout::timeout(60) do
        (NUM_DEPLOY_FORKS + 3).times do
          lines << read.readline
        end
      end
    ensure
      $stdout = STDOUT
    end
    # Close pipe
    read.close
    write.close

    # Take off the "deploying....done" stuff
    lines = lines[1..-1]

    # Token starts and ends up at the same place
    assert_equal(lines[0], lines[-1])

    # Token circulates amongst nodes
    for i in (1..lines.size-1)
      assert(lines[i]["Got token!"])
      for j in (i+1..lines.size-1)
        assert_not_equal(lines[i], lines[j])
      end
    end

    begin
      $stdout = StringIO.new
      ring_fork.stop_bg
      # Assert there are no child processes left; we've closed them all
      assert_equal(Process.waitall, [])
    ensure
      $stdout = STDOUT
    end
  end
end
