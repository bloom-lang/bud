require 'test_common'
require 'stringio' 


class TerminalTester
  include Bud
  state do
    table :saw_input
  end

  bloom do
    saw_input <= stdio
  end
end

class TestTerminal < Test::Unit::TestCase
  def XXXtest_stdin
    # I fail
    t = TerminalTester.new(:read_stdin => true)
    t.run_bg
    $stdin = StringIO.new("I am input from stdin")
    t.sync_do{}
    assert_equal(1, t.saw_input.length)
    t.stop_bg
  end
  
  def test_typing
    # I pass if the user actually hits return
    t = TerminalTester.new(:read_stdin => true)
    t.run_bg
    puts "please hit a key then press return in the next 3 seconds" 
    sleep 3
    assert_equal(1, t.saw_input.length)
    t.stop_bg
  end
end
