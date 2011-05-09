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
      (NUM_DEPLOY_FORKS + 2).times do
        lines << read.readline
      end
    end

    # Close pipe
    read.close
    write.close
    ring_fork.stop_bg

    # Assert there are no child processes left; we've closed them all
    assert_equal(Process.waitall, [])

    # First line is deploy status
    status_str = lines.shift
    assert_equal("Child nodes ready (count = #{NUM_DEPLOY_FORKS})\n", status_str)

    # Console output from different nodes might be intermixed in output pipe
    # (i.e., "lines" might not respect token delivery order). We just check that
    # each node got the token the same number of times.
    node_output = []
    lines.each do |l|
      m = l.match "^(\\d+): Got token!"
      assert(m)
      node_id = m[1].to_i
      node_output[node_id] ||= 0
      node_output[node_id] += 1
    end

    assert_equal(NUM_DEPLOY_FORKS, node_output.length)
    node_output.each_with_index do |n,i|
      if i == 0
        assert_equal(2, n)
      else
        assert_equal(1, n)
      end
    end
  end
end
