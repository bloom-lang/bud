require 'test_common'

class ExistTest
  include Bud

  state {
    table :notes
    table :memories
    table :dups, [:str]
    periodic :timer, 0.5
    channel :msgs
  }

  declare
  def program
    msgs <~ notes.map{|n| ["localhost:54321", n] if timer.exists?}
    memories <= msgs.payloads
    dups <= memories.map{|n| [n.inspect] if msgs.exists?{|m| n.val == m.val[1]}}
  end
end

class TestExists < Test::Unit::TestCase
  def test_conv
    p = ExistTest.new({:port=>54321})
    assert_nothing_raised(RuntimeError) { p.run_bg }
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.notes <+ [[1, 'what a lovely day']]
    }}
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.notes <+ [[2, 'I think I\'ll go for a walk']]
    }}
    sleep 1
    assert_nothing_raised(RuntimeError) { p.stop_bg }    
    assert_equal(2, p.memories.length)
    assert_equal('what a lovely day', p.memories.first.val)    
    assert_equal(2, p.dups.length)
  end
end