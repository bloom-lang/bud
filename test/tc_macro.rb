require 'test_common'

class SimpleMacroTest
  include Bud

  state do
    table :t1
    table :t2
    scratch :t3
    scratch :t4
    scratch :t5
  end

  bootstrap do
    t1 << [5, 10]
    t1 << [10, 20]
    t2 << [50, 100]
    t2 << [75, 125]
  end

  declare
  def rules
    j = join [t1, t2]
    t3 <= j.map {|a, b| [a.key + b.key, a.val + b.val]}
    t4 <= j.map {|a, b| [a.key + b.key, a.val + b.val]}
    t5 <= j.map {|a, b| a if b.val > 100}
  end
end

class TestMacros < Test::Unit::TestCase
  def test_simple
    p = SimpleMacroTest.new
    p.tick
    assert_equal([[55, 110], [60, 120], [80, 135], [85, 145]], p.t3.to_a.sort)
    assert_equal([[55, 110], [60, 120], [80, 135], [85, 145]], p.t4.to_a.sort)
    assert_equal([[5, 10], [10, 20]], p.t5.to_a.sort)
  end
end
