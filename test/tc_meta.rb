require 'test_common'

class LocalShortestPaths
  include Bud

  state {
    table :link, [:from, :to, :cost]
    table :link2, [:from, :to, :cost]
    table :empty, [:ident]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :minz, [:cost]
    table :minmaxsumcntavg, [:from, :to] => [:mincost, :maxcost, :sumcost, :cnt, :avgcost]
  }

  declare
  def program
    link2 <= link.map {|l| l unless empty.include? [l.ident]}
    path <= link2.map {|e| [e.from, e.to, e.to, e.cost]}
    j = join([link2, path])
    path <= j.map do |l, p|
      [l.from, p.to, p.from, l.cost+p.cost] if l.to == p.from
    end

    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

    minz <= shortest.group(nil, min(shortest.cost))
  end
end

class KTest
  include Bud

  state {
    interface input, :upd, [:datacol]
    interface input, :req, [:ident]
    interface output, :resp, [:ident, :datacol]
    table :mystate, [:datacol]
  }

  declare
  def update
    mystate <+ upd
    mystate <- join([upd, mystate]).map{|i, s| s}
  end

  declare
  def respond
    resp <= join([req, mystate]).map{|r, s| [r.ident, s.datacol]}
  end
end

class KTest2 < KTest
  declare
  def update
    mystate <= upd
    mystate <- join([upd, mystate]).map {|i, s| s}
  end
end


class KTest3 < KTest
  declare
  def update
    mystate <= upd.map {|u| u unless mystate.include? u}
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(4, program.strata.length)
  end

  def test_unstrat
    assert_raise(Bud::CompileError) { KTest3.new }
  end

  def test_visualization
    program = KTest2.new(:dump_rewrite => true, :trace => true)
    dep = DepAnalysis.new

    program.t_depends_tc.each {|d| dep.depends_tc << d}
    program.t_provides.each {|p| dep.providing << p}
    dep.tick
  end
end
