require 'test_common'

class SchemaFree
  include Bud

  state {
    table :notes
    scratch :stats
    interface input, :send_me
    channel :msgs
  }

  declare
  def program
    notes <= msgs.map{|msg| msg.val}
    msgs <~ send_me
  end
end

class TestBasic < Test::Unit::TestCase
  def test_bloom
    p = SchemaFree.new({:port=>54321})
    assert_nothing_raised(RuntimeError) { p.run_bg }
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.send_me <+ [['127.0.0.1:54321', [[123, 1], ['what a lovely day']]]]
    }}
    sleep 1
    assert_nothing_raised(RuntimeError) { p.stop_bg }    
    assert_equal(1, p.notes.length)
    assert_equal(123, p.notes.first.key[0])
    assert_equal(['what a lovely day'], p.notes.first.val)    
  end
end