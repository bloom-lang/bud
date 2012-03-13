require './test_common'

class DeltaTest
  include Bud

  state do
    table :orig, [:k1, :k2] => []
    scratch :scr, [:k1, :k2] => []
    table :result, [:k1, :k2] => []
  end

  bootstrap do
    orig <= [['a', 'b']]
    orig <= [['a', 'c']]
  end

  bloom do
    scr <= orig
    result <= scr
  end
end

class DeltaJoinTest
  include Bud

  state do
    table :orig, [:from, :to]
    scratch :link, [:from, :to]
    scratch :path, [:from, :to]
    scratch :hashpath, [:from, :to]
  end

  bootstrap do
    orig <= [['a', 'b'], ['b', 'c'], ['c', 'd']]
  end

  bloom :paths do
    link <= orig
    path <= link
    path <= (link * path).pairs {|l,p| [l.from, p.to] if l.to == p.from}
    hashpath <= link
    hashpath <= (link * path).pairs(:to => :from) {|l,p| [l.from, p.to]}
  end
end

class Delta3JoinTest
  include Bud

  state do
    table :orig, [:from, :to]
    table :wanted, [:node]
    scratch :link, [:from, :to]
    scratch :path, [:from, :to]
    scratch :hashpath, [:from, :to]
  end

  bootstrap do
    orig <= [['a', 'b'], ['b', 'c'], ['c', 'd']]
    wanted <= [['a'], ['b'], ['c']]
  end

  bloom :paths do
    link <= orig
    path <= link
    path <= (link * path * wanted).combos {|l,p,w| [l.from, p.to] if l.to == p.from and l.from == w.node}
    hashpath <= link
    hashpath <= (link * path * wanted).combos(link.to => path.from, link.from => wanted.node) {|l,p| [l.from, p.to]}
  end
end

class TestDelta < MiniTest::Unit::TestCase
  def test_transitivity
    program = DeltaTest.new
    program.tick
    assert_equal(2, program.result.length)
  end

  def test_one
    program = DeltaJoinTest.new
    program.tick
    assert_equal(6, program.path.length)
    assert_equal(6, program.hashpath.length)
  end

  def test_three
    program = Delta3JoinTest.new
    program.tick
    assert_equal(6, program.path.length)
    assert_equal(6, program.hashpath.length)
  end
end
