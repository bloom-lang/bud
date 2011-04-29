require 'test_common'
require 'stringio'
require '../examples/deploy/tokenring'
require 'timeout'

DEPLOY_NUM_NODES = 10

class RingThread
  include Bud
  include TokenRing
  include ThreadDeploy

  deploystrap do
    node_count << [DEPLOY_NUM_NODES]
  end

  bloom :pass_token_once do
    next_node <- (next_node * token_persist).lefts {|n| n.node}
  end
end

class TestThreadDeploy < Test::Unit::TestCase
  def test_thread_deploy
    read, write = IO.pipe
    $stdout = write
    deployer = RingThread.new(:deploy => true)
    deployer.run_bg

    lines = []
    begin
      Timeout::timeout(60) do
        (DEPLOY_NUM_NODES + 3).times do
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
      deployer.stop_bg
    ensure
      $stdout = STDOUT
    end
  end
end
