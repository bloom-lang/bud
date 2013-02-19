require './test_common'
require 'stringio' 


class StdinReader
  include Bud
  state do
    scratch :saw_input, stdio.schema
  end

  bloom do
    saw_input <= stdio
  end
end

class TestTerminal < MiniTest::Unit::TestCase
  def test_stdin
    input_lines = ["line1", "line2", "line3"]
    input_str = input_lines.join("\n") + "\n"
    input_buf = StringIO.new(input_str)
    q = Queue.new
    b = StdinReader.new(:stdin => input_buf)
    b.register_callback(:saw_input) do |tbl|
      tbl.to_a.each {|t| q.push(t)}
    end
    b.run_bg
    rv = []
    input_lines.length.times { rv << q.pop }
    assert_equal(input_lines.map{|l| [l]}.sort, rv.sort)
    b.stop
  end
end
