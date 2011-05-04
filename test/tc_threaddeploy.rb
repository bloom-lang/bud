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
    deploy_out = StringIO.new
    begin
      $stdout = deploy_out
      do_deploy_test
    ensure
      $stdout = STDOUT
    end
  end

  def do_deploy_test
    read, write = IO.pipe
    deployer = RingThread.new(:deploy => true, :stdout => write,
                              :deploy_child_opts => { :stdout => write })
    deployer.run_bg

    lines = []
    Timeout::timeout(45) do
      (DEPLOY_NUM_NODES + 1).times do |i|
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

    deployer.stop_bg
  end
end
