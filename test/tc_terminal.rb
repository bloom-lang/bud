require './test_common'
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

class TestTerminal < MiniTest::Unit::TestCase
  def test_stdin
    input_buf = StringIO.new("I am input from stdin\n")
    q = Queue.new
    t = TerminalTester.new(:stdin => input_buf)
    t.register_callback(:saw_input) do |tbl|
      q.push(tbl.length)
    end
    t.run_bg
    rv = q.pop
    assert_equal(1, rv)
    t.stop
  end
end
