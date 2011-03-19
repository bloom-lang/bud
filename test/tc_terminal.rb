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
  def test_stdin
    $stdin = StringIO.new("I am input from stdin\n")
    t = TerminalTester.new(:read_stdin => true)
    t.run_bg
    # XXX: kludge. We're waiting for the stdin reader thread to send data to the
    # main Bud thread via UDP on the loopback interface; in the absence of
    # proper callbacks, just spin a few times.
    10.times do
      t.sync_do
    end
    assert_equal(1, t.saw_input.length)
    t.stop_bg
  end
end
