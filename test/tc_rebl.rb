require './test_common'
require 'bud/rebl'
require 'stringio'
require 'timeout'

def capture_stdout
  $stdout = StringIO.new("", "w")
  begin
    yield 
    $stdout.string
  ensure
    $stdout = STDOUT
  end
end

class ReblTester
  attr_reader :lib

  def initialize
    @lib = ReblShell::setup
  end

  def exec_rebl(str)
    return capture_stdout do
      $stdin = StringIO.new(str)
      ReblShell::rebl_loop(@lib, true)
    end
  end
end

# TODO: add the following testcases:
#    * test persistent store functionality

class TestRebl < MiniTest::Unit::TestCase
  def test_rebl_pingpong
    the_line = nil
    rt1 = nil
    ip_port1 = nil
    rt2 = nil
    ip_port2 = nil
    # Ignore the welcome messages.
    capture_stdout do
      rt1 = ReblTester.new
      rt2 = ReblTester.new
      ip_port1 = "#{rt1.lib.ip}:#{rt1.lib.port}"
      ip_port2 = "#{rt2.lib.ip}:#{rt2.lib.port}"
    end

    # Set up ping rules, send initial ping from rt1 to rt2
    rt1.exec_rebl("channel :ping, [:@dst, :src]")
    rt1.exec_rebl("ping <~ ping.map {|p| [p.src, p.dst]}")
    rt2.exec_rebl("channel :ping, [:@dst, :src]")
    rt2.exec_rebl("ping <~ ping.map {|p| [p.src, p.dst]}")
    rt1.exec_rebl("ping <~ [['#{ip_port2}', ip_port]]")
    rt1.exec_rebl("stdio <~ [(@budtime == 50) ? ['hit'] : nil]")
    rt2.exec_rebl("/run")

    # Start up the node, and wait for the bud time to go up to 50 (non-lazy mode)
    begin
      read, $stdout = IO.pipe
      $stdin = StringIO.new("/run")
      ReblShell::rebl_loop(rt1.lib, true)
      Timeout::timeout(30) do
        the_line = read.readline
      end
    ensure
      $stdout = STDOUT
    end
    assert_equal("hit\n", the_line)

    # Now perform a stop on both nodes
    rt1.exec_rebl("/stop")
    rt2.exec_rebl("/stop")

    # Check their timestamps
    stop_time1 = rt1.lib.rebl_class_inst.budtime
    stop_time2 = rt2.lib.rebl_class_inst.budtime

    begin
      # Now, test the breakpoint functionality
      rt1.exec_rebl("rebl_breakpoint <= [{50 => [true]}[@budtime]]")
      rt2.exec_rebl("/run")
      read, $stdout = IO.pipe
      $stdin = StringIO.new("/run")
      ReblShell::rebl_loop(rt1.lib, true)
      Timeout::timeout(30) do
        the_line = read.readline
      end
    ensure
      $stdout = STDOUT
    end
    assert_equal("hit\n", the_line)

    # Now perform a stop on both nodes
    rt1.exec_rebl("/stop")
    rt2.exec_rebl("/stop")

    # Check their timestamps
    stop_time1 = rt1.lib.rebl_class_inst.budtime
    stop_time2 = rt2.lib.rebl_class_inst.budtime
  end

  def start_tester
    rt = nil
    # Ignore the welcome messages.
    capture_stdout do
      rt = ReblTester.new
    end
    return rt
  end

  def test_rebl_shortestpaths
    rt = start_tester

    # Check to see if help mode works
    rt.exec_rebl("/help")

    # Declarations
    rt.exec_rebl("table :link, [:from, :to, :cost]")
    rt.exec_rebl("table :path, [:from, :to, :nxt, :cost]")

    # Check lscollections
    expected_output = "1: table :link, [:from, :to, :cost]\n2: table :path, [:from, :to, :nxt, :cost]\n"
    actual_output = rt.exec_rebl("/lscollections")
    assert_equal(expected_output, actual_output)

    # Now add some rules
    rt.exec_rebl("path <= link {|e| [e.from, e.to, e.to, e.cost]}")
    rt.exec_rebl("temp :k <= (link*path).pairs(:to => :from)")
    rt.exec_rebl("path <= k { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }")
    rt.exec_rebl("stdio <~ [['foo']]")
    actual_output = rt.exec_rebl("/tick 3")

    # Check to make sure stdio thing is printing
    assert_equal("foo\nfoo\nfoo\n", actual_output)
    rt.exec_rebl("/rmrule 4")
    actual_output = rt.exec_rebl("/tick 3")
    # Check to make sure removed stdio rule no longer prints
    assert_equal("", actual_output)

    # Now check the rules we've got
    expected_output = "1: path <= link {|e| [e.from, e.to, e.to, e.cost]}\n2: temp :k <= (link*path).pairs(:to => :from)\n3: path <= k { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }\n"
    actual_output = rt.exec_rebl("/lsrules")
    assert_equal(expected_output, actual_output)

    # Now add some links and tick
    rt.exec_rebl("link <= [['a','b',1],['a','b',4],['b','c',1],['c','d',1],['d','e',1]]")
    rt.exec_rebl("/tick")

    # Check dump functionality
    expected_output = "[\"a\", \"b\", \"b\", 1]\n[\"a\", \"b\", \"b\", 4]\n[\"a\", \"c\", \"b\", 2]\n[\"a\", \"c\", \"b\", 5]\n[\"a\", \"d\", \"b\", 3]\n[\"a\", \"d\", \"b\", 6]\n[\"a\", \"e\", \"b\", 4]\n[\"a\", \"e\", \"b\", 7]\n[\"b\", \"c\", \"c\", 1]\n[\"b\", \"d\", \"c\", 2]\n[\"b\", \"e\", \"c\", 3]\n[\"c\", \"d\", \"d\", 1]\n[\"c\", \"e\", \"d\", 2]\n[\"d\", \"e\", \"e\", 1]\n"
    actual_output = rt.exec_rebl("/dump path")
    assert_equal(expected_output, actual_output)

    # Add a new collection and rule for shortest paths, and tick
    rt.exec_rebl("table :shortest, [:from, :to] => [:nxt, :cost]")
    rt.exec_rebl("shortest <= path.argmin([path.from, path.to], path.cost)")
    rt.exec_rebl("/tick")

    # Now, remove all of the rules, and tick
    rt.exec_rebl("/rmrule 4")
    rt.exec_rebl("/rmrule 3")
    rt.exec_rebl("/rmrule 1")
    rt.exec_rebl("/rmrule 2")
    rt.exec_rebl("/tick")

    # Now check the contents of shortest to make sure that rule removal doesn't
    # cause un-derivation of previously derived tuples
    expected_output = "[\"a\", \"b\", \"b\", 1]\n[\"a\", \"c\", \"b\", 2]\n[\"a\", \"d\", \"b\", 3]\n[\"a\", \"e\", \"b\", 4]\n[\"b\", \"c\", \"c\", 1]\n[\"b\", \"d\", \"c\", 2]\n[\"b\", \"e\", \"c\", 3]\n[\"c\", \"d\", \"d\", 1]\n[\"c\", \"e\", \"d\", 2]\n[\"d\", \"e\", \"e\", 1]\n"
    actual_output = rt.exec_rebl("/dump shortest")
    assert_equal(expected_output, actual_output)
  end

  # Issue 274 -- check that cached state is invalidated correctly when
  # reinstanciating Bud.
  def test_no_leftovers
    rt = start_tester
    rt.exec_rebl("scratch :passing_clouds")
    rt.exec_rebl(%{passing_clouds <= [[3, "Nimbus"], [2, "Cumulonimbus"]]})
    rt.exec_rebl(%{stdio <~ passing_clouds.inspected})
    actual_output = rt.exec_rebl("/tick")
    assert_match(/3.*Nimbus/, actual_output)
    assert_match(/2.*Cumulonimbus/, actual_output)
    rt.exec_rebl("/rmrule 1")
    actual_output = rt.exec_rebl("/tick")
    assert_match(/\s*/, actual_output)
  end

  def test_rebl_lattice
    rt = start_tester
    rt.exec_rebl("lset :s1")
    rt.exec_rebl("lmax :m1")

    expected = "1: lset :s1\n2: lmax :m1\n"
    assert_equal(expected, rt.exec_rebl("/lscollections"))

    rt.exec_rebl("m1 <= s1.size")
    rt.exec_rebl("s1 <= [[1], [2], [3]]")
    assert_equal(expected, rt.exec_rebl("/lscollections"))

    rt.exec_rebl("/tick")
    assert_equal("<lmax: 3>\n", rt.exec_rebl("/dump m1"))

    # We expect that removing a rule does that remove previously derived
    # conclusions
    rt.exec_rebl("/rmrule 1")
    rt.exec_rebl("s1 <= [[4], [5], [6]]")
    rt.exec_rebl("/tick")
    assert_equal("<lmax: 3>\n", rt.exec_rebl("/dump m1"))
    assert_equal("<lset: [1, 2, 3, 4, 5, 6]>\n", rt.exec_rebl("/dump s1"))
  end
end
