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

class MacroRefMacro
  include Bud

  state do
    table :t1
    scratch :t2
  end

  bootstrap do
    t1 << [100, 200]
  end

  declare
  def rules
    a = t1.map {|t| [t.key + 10, t.val + 10]}
    b = a.map {|t| [t[0] + 20, t[1] + 20]}
    c = b.map {|t| [t[0] - 50, t[1] - 100]}
    d = b.map {|t| [t[0] - 50, t[1] - 100]} # unreferenced
    t2 <= c
  end
end

class MacroShadow
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
  end

  bootstrap do
    t1 << [20,40]
    t1 << [40,60]
  end

  declare
  def rules
    k = t1.map {|t| [t.key + 10, t.val + 20]}
    t2 <= k
    t2 <= t1.map {|k| [k.key, k.val]}
    t3 <= join([t1, t2], [t1.key, t2.key]).map {|j,k| [j.key + 20, k.val + 20]}
    t4 <= t3.map {|t| [t.key, t.val] unless k.include? t}
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

  def test_macro_in_macro
    p = MacroRefMacro.new
    p.tick
    assert_equal([[80, 130]], p.t2.to_a.sort)
  end

  def test_macro_shadow
    p = MacroShadow.new
    p.tick
    assert_equal([[20, 40], [30, 60], [40, 60], [50, 80]], p.t2.to_a.sort)
    assert_equal([[40, 60], [60, 80]], p.t3.to_a.sort)
    assert_equal([[20, 40]], p.t4.to_a.sort)
  end
end
