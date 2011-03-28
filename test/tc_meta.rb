require 'test_common'

class LocalShortestPaths
  include Bud

  state do
    table :link, [:from, :to, :cost]
    table :link2, [:from, :to, :cost]
    table :link3, [:from, :to, :cost]
    table :empty, [:ident]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :minz, [:cost]
    table :minmaxsumcntavg, [:from, :to] => [:mincost, :maxcost, :sumcost, :cnt, :avgcost]
  end

  bloom do
    link2 <= link.map {|l| l unless empty.include? [l.ident]}
    path <= link2.map {|e| [e.from, e.to, e.to, e.cost]}
    temp :j <= join([link2, path])
    path <= j.map do |l, p|
      [l.from, p.to, p.from, l.cost+p.cost] if l.to == p.from
    end

    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

    minz <= shortest.group(nil, min(shortest.cost))

    link3 <= path.map {|p| [p.from, p.to, p.cost]}
    link3 <- join([link3, shortest], [link3.from, shortest.from], [link3.to, shortest.to]).map {|l, s| l }
  end
end

class KTest
  include Bud

  state do
    interface input, :upd, [:datacol]
    interface input, :req, [:ident]
    interface output, :resp, [:ident, :datacol]
    table :mystate, [:datacol]
  end

  bloom :update do
    mystate <+ upd
    mystate <- join([upd, mystate]).map{|i, s| s}
  end

  bloom :respond do
    resp <= join([req, mystate]).map{|r, s| [r.ident, s.datacol]}
  end
end

class KTest2 < KTest
  state do
    # make sure :node isn't reserved
    scratch :node
  end
  bloom :update do
    mystate <= upd
    node <= upd
    temp :j <= join([upd, mystate])
    mystate <- j.map {|i, s| s}
  end
end


class KTest3 < KTest
  bloom :update do
    mystate <= upd.map {|u| u unless mystate.include? u}
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_equal(4, program.strata.length)

    tally = 0
    program.t_depends.each do |dep|
      if dep.lhs == "shortest" and dep.body == "path"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "minz" and dep.body == "shortest"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "minmaxsumcntavg" and dep.body == "path"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "link2" and dep.body == "empty"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "link3" and dep.body == "shortest"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "link3" and dep.body == "link3"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.body == "count"
        # weird: count is now getting parsed as a table
      else
        assert(!dep.nm, "Monotonic rule marked NM: #{dep.inspect}")
      end 
    end
    assert_equal(6, tally)
  end

  def test_unstrat
    assert_raise(Bud::CompileError) { KTest3.new }
  end

  def test_visualization
    program = KTest2.new(:trace => true)
    dep = DepAnalysis.new

    program.run_bg
    program.sync_do
    program.sync_do
    program.sync_do

    program.t_depends_tc.each {|d| dep.depends_tc << d}
    program.t_provides.each {|p| dep.providing << p}
    dep.tick
  end
end
