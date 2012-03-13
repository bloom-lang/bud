require './test_common'

module SimpleModule
  state do
    table :boot_tbl
  end

  bootstrap do
    boot_tbl << [25, 50]
  end
end

class ParentBud
  include Bud
  include SimpleModule

  state {
    table :tbl
  }

  bootstrap do
    boot_tbl << [5, 10]
  end

  bloom :bundle do
    tbl <= [[2, 'a']]
  end
end

class ChildBud < ParentBud
  bootstrap do
    boot_tbl << [10, 20]
  end

  # Test overriding
  bloom :bundle do
    tbl <= [[2, 'b']]
  end
end

class TestSubclass < MiniTest::Unit::TestCase
  def test_override
    p1 = ParentBud.new
    p2 = ChildBud.new
    p1.tick
    p2.tick

    assert_equal('a', p1.tbl[[2]].val)
    assert_equal('b', p2.tbl[[2]].val)

    assert_equal([[5, 10], [25, 50]], p1.boot_tbl.to_a.sort)
    assert_equal([[5, 10], [10, 20], [25, 50]], p2.boot_tbl.to_a.sort)
  end
end
