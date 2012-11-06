require './test_common'

class SchemaFree
  include Bud

  state do
    table :notes
    scratch :stats
    interface input, :send_me
    channel :msgs
  end

  bloom do
    notes <= msgs.payloads {|p| p.val}
    msgs <~ send_me
  end
end

class TestSFree < MiniTest::Unit::TestCase
  def test_bloom
    p = SchemaFree.new
    p.run_bg

    q = Queue.new
    p.register_callback(:msgs) do
      q.push(true)
    end

    p.sync_do {
      p.send_me <+ [[p.ip_port, [[123, 1], 'what a lovely day']]]
    }
    p.sync_do {
      p.send_me <+ [[p.ip_port, [[123, 2], "I think I'll go for a walk"]]]
    }

    2.times { q.pop }

    p.stop
    assert_equal(2, p.notes.length)
    assert_equal(123, p.notes.first.key[0])
    assert_equal('what a lovely day', p.notes.first.val)
  end
end
