require 'test_common'
require 'stringio' 


class TerminalTester
  include Bud
  state do
    scratch :saw_input
  end

  bloom do
    saw_input <= stdio
  end
end

class TestTerminal < Test::Unit::TestCase
  def test_stdin
    input_buf = StringIO.new("I am input from stdin\n")
    q = Queue.new
    t = TerminalTester.new(:stdin => input_buf)
    t.register_callback(:saw_input) do |tbl|
      assert_equal(1, tbl.length)
      q.push(true)
    end
    t.run_bg
    q.pop
    t.stop_bg
  end
end
