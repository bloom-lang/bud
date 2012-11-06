require './test_common'

class ExistTest
  include Bud

  state do
    table :notes
    table :memories
    table :dups, [:str]
    periodic :timer, 0.5
    channel :msgs
  end

  bloom do
    msgs <~ (notes * timer).lefts{|n| [ip_port, n]}
    memories <= msgs.payloads{|p| p.val}
    dups <= memories.map{|n| [n.inspect] if msgs.exists?{|m| n.val == m.val[1]}}
  end
end

class TestExists < MiniTest::Unit::TestCase
  def test_conv
    p = ExistTest.new
    p.run_bg

    q = Queue.new
    p.register_callback(:msgs) do
      q.push(true)
    end

    p.sync_do {
      p.notes <+ [[1, 'what a lovely day']]
    }
    p.sync_do {
      p.notes <+ [[2, "I think I'll go for a walk"]]
    }

    # Wait for two messages
    2.times { q.pop }

    p.stop
    assert_equal(2, p.memories.length)
    assert_equal('what a lovely day', p.memories.first.val)
    assert_equal(2, p.dups.length)
  end
end
