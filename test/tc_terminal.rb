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

class StdioEcho
  include Bud

  bloom do
    stdio <~ stdio {|s| ["Saw: #{s.line}"]}
  end
end

class StdioBootstrap
  include Bud

  bootstrap do
    stdio <~ [["hello from bootstrap!"]]
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

  def test_stdio_pipe
    in_read, in_write = IO.pipe
    out_read, out_write = IO.pipe

    b = StdioEcho.new(:stdin => in_read, :stdout => out_write)
    b.run_bg

    ["foo", "bar", "baz"].each do |str|
      in_write.puts(str)
      rv = out_read.gets
      assert_equal("Saw: #{str}\n", rv)
    end

    b.stop
  end

  def test_stdio_bootstrap
    output_buf = StringIO.new
    b = StdioBootstrap.new(:stdout => output_buf)
    b.tick
    assert_equal("hello from bootstrap!\n", output_buf.string)
  end
end
