require 'test_common'

class ParentBud
  include Bud

  state {
    table :tbl, [:k] => [:v]
  }
  
  declare
  def bundle
    tbl << [2, 'a']
  end
end

class ChildBud < ParentBud
  # Test overriding
  declare
  def bundle
    tbl << [2, 'b']
  end
end

class TestSubclass < Test::Unit::TestCase
  def test_override
    p1 = ParentBud.new
    p2 = ChildBud.new
    assert_nothing_raised(RuntimeError) { p1.tick }
    assert_nothing_raised(RuntimeError) { p2.tick }
    assert_equal('a', p1.tbl[[2]][1])
    assert_equal('b', p2.tbl[[2]][1])
  end
end
