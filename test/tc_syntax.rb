require 'test_common'

module SyntaxBase
  state do
    table :foo 
    table :bar
  end
end

class SyntaxTest1
  include Bud
  include SyntaxBase

  bloom :foobar do
    foo = bar
  end
end

class TestSyntax < Test::Unit::TestCase
  def test_parsetime_error
    begin
      SyntaxTest1.new
      assert(false, "Should have raised an exception")
    rescue
      assert_equal(Bud::CompileError, $!.class)
      # fragile assertion? (whitespace etc)
      assert_equal("Error parsing rule block __bloom__foobar:\ndef __bloom__foobar\n  foo = bar\nend", $!.to_s)
    end
  end
end
