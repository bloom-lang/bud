require 'test_common'

class SchemaFree
  include Bud

  state do
    table :notes
    scratch :stats
    interface input, :send_me
    channel :msgs
  end

  bloom do
    notes <= msgs.payloads
    msgs <~ send_me
    # stdio <~ msgs.inspected
  end
end

class TestSFree < Test::Unit::TestCase
  def test_bloom
    p = SchemaFree.new({:port=>54321})
    assert_nothing_raised(RuntimeError) { p.run_bg }
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.send_me <+ [['127.0.0.1:54321', [[123, 1], 'what a lovely day']]]
    }}
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.send_me <+ [['127.0.0.1:54321', [[123, 2], 'I think I\'ll go for a walk']]]
    }}
    sleep 1
    assert_nothing_raised(RuntimeError) { p.stop_bg }    
    assert_equal(2, p.notes.length)
    assert_equal(123, p.notes.first.key[0])
    assert_equal('what a lovely day', p.notes.first.val)    
  end
end
