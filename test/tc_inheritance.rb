# variable design still rather tentative

class ParentBud < Bud
  def state
    table :tbl, ['k'], ['v']
  end
  
  def declaration
  end
  
  declare
  def bundle
    tbl << [2, 'a']
  end
end

class ChildBud < ParentBud
  declare
  def bundle
    tbl << [2, 'b']
  end
end


class TestSubclass < Test::Unit::TestCase
  def test_override
    p1 = ParentBud.new('localhost', 12345)
    p2 = ChildBud.new('localhost', 12346)
    assert_nothing_raised( RuntimeError) { p1.tick }
    assert_nothing_raised( RuntimeError) { p2.tick }
    assert_equal('a', p1.tbl[[2]][1])
    assert_equal('b', p2.tbl[[2]][1])
  end
end
