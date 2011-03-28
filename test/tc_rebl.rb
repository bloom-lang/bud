require 'test_common'
require 'stringio'
require 'bud/rebl'

class ReblTester
  def initialize
    @lib = ReblShell::setup
  end

  def exec_rebl(str)
    out = StringIO.new
    $stdout = out
    $stdin = StringIO.new(str)
    ReblShell::rebl_loop(@lib, true)
    return out.string
  ensure
    $stdout = STDOUT
  end
end

# TODO: add the following testcases:
#    * test persistent store functionality
#    * ping/pong test with multiple rebls

class TestRebl < Test::Unit::TestCase
  def test_rebl_shortestpaths
    rt = nil
    actual_output = nil
    assert_nothing_raised do
      begin
        # Ignore the welcome messages.
        $stdout = StringIO.new
        rt = ReblTester.new
      ensure
        $stdout = STDOUT
      end
    end

    # Check to see if help mode works
    assert_nothing_raised do
      rt.exec_rebl("/help")
    end

    # Declarations
    assert_nothing_raised do
      rt.exec_rebl("table :link, [:from, :to, :cost]")
      rt.exec_rebl("table :path, [:from, :to, :next, :cost]")
    end

    # Check lscollections
    expected_output = "1: table :link, [:from, :to, :cost]\n2: table :path, [:from, :to, :next, :cost]\n"
    assert_nothing_raised do
      actual_output = rt.exec_rebl("/lscollections")
    end
    assert_equal(expected_output, actual_output)

    # Now add some rules
    assert_nothing_raised do
      rt.exec_rebl("path <= link {|e| [e.from, e.to, e.to, e.cost]}")
      rt.exec_rebl("temp :j <= (link*path).pairs(:to => :from)")
      rt.exec_rebl("path <= j { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }")
      rt.exec_rebl("stdio <~ [['foo']]")
      actual_output = rt.exec_rebl("/tick 3")
    end
    # Check to make sure stdio thing is printing
    assert_equal("foo\nfoo\nfoo\n", actual_output)
    assert_nothing_raised do
      rt.exec_rebl("/rmrule 4")
      actual_output = rt.exec_rebl("/tick 3")
    end
    # Check to make sure removed stdio rule no longer prints
    assert_equal("", actual_output)

    # Now check the rules we've got
    expected_output = "1: path <= link {|e| [e.from, e.to, e.to, e.cost]}\n2: temp :j <= (link*path).pairs(:to => :from)\n3: path <= j { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }\n"
    assert_nothing_raised do
      actual_output = rt.exec_rebl("/lsrules")
    end
    assert_equal(expected_output, actual_output)

    # Now add some links and tick
    assert_nothing_raised do
      rt.exec_rebl("link <= [['a','b',1],['a','b',4],['b','c',1],['c','d',1],['d','e',1]]")
      rt.exec_rebl("/tick")
    end

    # Check dump functionality
    expected_output = "[\"a\", \"b\", \"b\", 1]\n[\"a\", \"b\", \"b\", 4]\n[\"a\", \"c\", \"b\", 2]\n[\"a\", \"c\", \"b\", 5]\n[\"a\", \"d\", \"b\", 3]\n[\"a\", \"d\", \"b\", 6]\n[\"a\", \"e\", \"b\", 4]\n[\"a\", \"e\", \"b\", 7]\n[\"b\", \"c\", \"c\", 1]\n[\"b\", \"d\", \"c\", 2]\n[\"b\", \"e\", \"c\", 3]\n[\"c\", \"d\", \"d\", 1]\n[\"c\", \"e\", \"d\", 2]\n[\"d\", \"e\", \"e\", 1]\n"
    assert_nothing_raised do
      actual_output = rt.exec_rebl("/dump path")
    end
    assert_equal(expected_output, actual_output)

    # Add a new collection and rule for shortest paths, and tick
    assert_nothing_raised do
      rt.exec_rebl("table :shortest, [:from, :to] => [:next, :cost]")
      rt.exec_rebl("shortest <= path.argmin([path.from, path.to], path.cost)")
      rt.exec_rebl("/tick")
    end

    # Now, remove all of the rules, and tick
    assert_nothing_raised do
      rt.exec_rebl("/rmrule 4")
      rt.exec_rebl("/rmrule 3")
      rt.exec_rebl("/rmrule 1")
      rt.exec_rebl("/rmrule 2")
      rt.exec_rebl("/tick")
    end

    # Now check the contents of shortest to make sure that rule removal doesn't
    # cause un-derivation of previously derived tuples
    expected_output = "[\"a\", \"b\", \"b\", 1]\n[\"a\", \"c\", \"b\", 2]\n[\"a\", \"d\", \"b\", 3]\n[\"a\", \"e\", \"b\", 4]\n[\"b\", \"c\", \"c\", 1]\n[\"b\", \"d\", \"c\", 2]\n[\"b\", \"e\", \"c\", 3]\n[\"c\", \"d\", \"d\", 1]\n[\"c\", \"e\", \"d\", 2]\n[\"d\", \"e\", \"e\", 1]\n"
    assert_nothing_raised do
      actual_output = rt.exec_rebl("/dump shortest")
    end
    assert_equal(expected_output, actual_output)
  end
end
