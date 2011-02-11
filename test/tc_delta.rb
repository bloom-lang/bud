require 'test_common'

class DeltaTest < Bud
  def state
    table :orig, [:k1, :k2] => [:v1, :v2]
    scratch :scr, [:k1, :k2]
    table :result, [:k1, :k2] => [:v1, :v2]
  end

  def bootstrap
    orig <= [['a', 'b', 1, 2]]
    orig <= [['a', 'c', 3, 4]]
  end

  declare
  def rules
    scr <= orig
    result <= scr
  end
end

class DeltaJoinTest < Bud
  def state
    table :orig, [:from, :to]
    scratch :link, [:from, :to]
    scratch :path, [:from, :to]
    scratch :hashpath, [:from, :to]
  end
  def bootstrap
    orig <= [['a', 'b'], ['b', 'c'], ['c', 'd']]
  end

  declare
  def paths
    link <= orig
    path <= link
    path <= join([link, path]).map {|l,p| [l.from, p.to] if l.to == p.from}
    hashpath <= link
    hashpath <= join([link, path], [link.to, path.from]).map {|l,p| [l.from, p.to]}
  end
end

class Delta3JoinTest < Bud
  def state
    table :orig, [:from, :to]
    table :wanted, [:node]
    scratch :link, [:from, :to]
    scratch :path, [:from, :to]
    scratch :hashpath, [:from, :to]
  end
  def bootstrap
    orig <= [['a', 'b'], ['b', 'c'], ['c', 'd']]
    wanted <= [['a'], ['b'], ['c']]
  end

  declare
  def paths
    link <= orig
    path <= link
    path <= join([link, path, wanted]).map{|l,p,w| [l.from, p.to] if l.to == p.from and l.from == w.node}
    hashpath <= link
    hashpath <= join([link, path, wanted], [link.to, path.from],[link.from, wanted.node]).map {|l,p| [l.from, p.to]}
  end
end

class TestDelta < Test::Unit::TestCase
  def test_transitivity
    program = DeltaTest.new
    assert_nothing_raised(RuntimeError) {program.tick}
    assert_equal(2, program.result.length)
  end

  def test_one
    program = DeltaJoinTest.new
    assert_nothing_raised(RuntimeError) {program.tick}
    assert_equal(6, program.path.length)
    assert_equal(6, program.hashpath.length)
  end

  def test_three
    program = Delta3JoinTest.new
    assert_nothing_raised(RuntimeError) {program.tick}
    assert_equal(6, program.path.length)
    assert_equal(6, program.hashpath.length)
  end
end
