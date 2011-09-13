require 'test_common'
require 'bud/graphs.rb'
require 'bud/viz_util.rb'

include VizUtil

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
    path <= (link2 * path).pairs do |l, p|
      [l.from, p.to, p.from, l.cost+p.cost] if l.to == p.from
    end

    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

    minz <= shortest.group(nil, min(shortest.cost))

    link3 <= path.map {|p| [p.from, p.to, p.cost]}
    link3 <- (link3 * shortest).lefts(:from => :from, :to => :to)
  end
end

class Underspecified
  include Bud

  state do
    interface input, :iin
    interface output, :iout
  end
end

class KTest
  include Bud

  state do
    interface input, :sinkhole
    interface input, :upd, [:datacol]
    interface input, :req, [:ident]
    interface output, :resp, [:ident, :datacol]
    table :mystate, [:datacol]
  end

  bloom :update do
    mystate <+ upd
    mystate <- (upd * mystate).rights
  end

  bloom :respond do
    resp <= (req * mystate).pairs {|r, s| [r.ident, s.datacol]}
  end
end

class KTest2 < KTest
  state do
    # make sure :node isn't reserved
    scratch :noder
  end
  bloom :update do
    mystate <= upd
    noder <= upd
    temp :interm <= mystate
    mystate <- (upd * interm).rights
  end
end


class KTest3 < KTest
  bloom :update do
    mystate <= upd.map {|u| u unless mystate.include? u}
  end
end

class TestStratTemporal
  include Bud

  state do
    scratch :foo, [:loc, :a]
    table :foo_persist, [:loc, :a]
    scratch :foo_cnt, [:a] => [:cnt]
  end

  bootstrap do
    foo <= [["xyz",1], ["xyz",2], ["xyz",3]]
  end

  bloom do
    foo_persist <= foo
    foo_cnt <= foo_persist.group([:loc], count)

    foo_persist <- ((if foo_cnt[["xyz"]] and
                        foo_cnt[["xyz"]].cnt == 3
                       foo_persist
                     end) or [])
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new
    assert_equal(5, program.strata.length)

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

  def scratch_dir
    dir = '/tmp/' + Time.new.to_f.to_s
    Dir.mkdir(dir)
    return dir
  end

  def test_visualization
    program = KTest2.new(:trace => true, :dump_rewrite => true, :port => 54321)
    File.delete("KTest2_rewritten.txt")
    `rm -r DBM_KTest2*`
    dir = scratch_dir

    program.run_bg
    program.sync_do{}
    
    write_graphs({}, program.t_cycle, program.t_depends, program.t_rules, "#{dir}/test_viz", dir, :dot, false, nil, 1, {})
    program.stop_bg
  end

  def test_plotting
    program = KTest2.new(:output => :dot)
    dep = DepAnalysis.new

    program.run_bg
    program.sync_do
    program.sync_do
    program.sync_do

    program.t_depends_tc.each {|d| dep.depends_tc << d}
    program.t_provides.each {|p| dep.providing << p}
    dep.tick
    dir = scratch_dir
    graph_from_instance(program, "#{dir}/test_graphing", dir, true, :dot)
    fp = File.open("#{dir}/test_graphing.svg", "r")
    content = ''
    while (s = fp.gets)
      content += s
    end
    fp.close
  
    assert_match(/n_upd -> \"n_interm, mystate\" \[label=\" \+\/\-\",.+?arrowhead=veeodot/, content)
    assert_match("n_S -> n_upd", content)
    assert_match("n_S -> n_req", content)
    assert_match("n_sinkhole -> \"n_\?\?\"", content)
    assert_no_match(/n_upd -> \"n_\?\?\"/, content)
    assert_no_match(/n_req -> \"n_\?\?\"/, content)
    `rm -r #{dir}`
    program.stop_bg
  end

  def test_underspecified
    u = Underspecified.new
    assert_equal(2, u.t_underspecified.length)
    u.t_underspecified.each do |u|
      case u[0]
        when "iin" then assert(u[1])
        when "iout" then assert(!u[1])
      end
    end
  end

  def test_temporal_strat
    t = TestStratTemporal.new
    assert_equal(3, t.strata.length)
    t.tick
    assert_equal([["xyz", 1], ["xyz", 2], ["xyz", 3]], t.foo_persist.to_a.sort)
    t.tick
    assert_equal([], t.foo_persist.to_a.sort)
  end

  def test_spacetime
    inp = [
      [:ping, :a, :b, 1, 3],
      [:pong, :b, :a, 4, 2],
      [:ping, :a, :b, 3, 5],
      [:pong, :b, :a, 6, 4]
    ];
    
    do_spacetime(inp, "foofoo")
  end

  def test_spacetime2
    inp = [
      [:ping, :a, :b, 1, 3],
      [:ping, :b, :c, 4, 11],
      [:pong, :c, :b, 12, 5],
      [:pong, :b, :a, 6, 2]
    ]
    do_spacetime(inp, "foo2")
  end

  def test_spacetime3
    inp = [
      [:ping1, :a, :b, 1, 3],
      [:ping2, :a, :b, 1, 4],
      [:ping3, :a, :b, 1, 5],
      [:ping4, :a, :b, 1, 6]
    ]
    do_spacetime(inp, "foo3")
  end
  
  def do_spacetime(inp, out)
    st = SpaceTime.new(inp)
    st.process
    st.finish(out)
  end
end

class TestThetaMeta < Test::Unit::TestCase
  class ThetaMonotoneJoin
    include Bud

    state do
      table :a
      table :b
      table :c
    end

    bloom do
      c <= (a * b).pairs(a.key => b.key)
    end
  end
  
  def test_theta
    p = ThetaMonotoneJoin.new
    p.t_depends.each do |dep|
      assert(!dep[4], "this dependency should't be marked nonmonotonic: #{dep.inspect}")
    end
  end
end
