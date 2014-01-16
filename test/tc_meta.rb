require './test_common'
require 'bud/graphs.rb'
require 'bud/viz_util.rb'
require 'bud/depanalysis'
require 'bud/meta_algebra'

include VizUtil

class LocalShortestPaths
  include Bud

  state do
    table :link, [:from, :to, :cost]
    table :link2, [:from, :to, :cost]
    table :link3, [:from, :to, :cost]
    table :empty, [:ident]
    table :path, [:from, :to, :nxt, :cost]
    table :shortest, [:from, :to] => [:nxt, :cost]
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


class Divergence
  include Bud

  include MetaAlgebra
  include MetaReports

  state do
    interface input, :cli1, [:data]
    channel :chan, [:@loc, :data]
    table :serv1, chan.schema
    scratch :agg, [:data, :cnt]
    channel :outs, [:@loc, :data, :cnt]
    interface output, :gone, outs.schema
  end

  bloom do
    chan <~ cli1{|c| ["localhost:12345", c.data]}
    serv1 <= chan
    agg <= serv1.group([serv1.data], count)

    outs <~ agg{|a| ["localhost:12345", a.data, a.cnt]}
    gone <= outs
  end
end

class InterfaceAlternate
  include Bud
  state do
    channel :inc
    channel :outc
    table :stor
    interfaces :input, [:inc, :stor]
    interfaces :output, [:outc]
  end
end

class TestMeta < MiniTest::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new
    assert_equal(4, program.stratified_rules.length)

    tally = 0
    program.t_depends.each do |dep|
      next if VizUtil.ma_tables.keys.include? dep.lhs.to_sym
      assert_equal(false, dep.in_body) unless dep.lhs == "link2" and dep.body == "empty"
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
        assert(dep.in_body)
        tally += 1
      elsif dep.lhs == "link3" and dep.body == "shortest"
        assert(dep.nm, "NM rule")
        tally += 1
      elsif dep.lhs == "link3" and dep.body == "link3"
        assert(dep.nm, "NM rule")
        tally += 1
      else
        assert(!dep.nm, "Monotonic rule marked NM: #{dep.inspect}")
      end
    end
    assert_equal(6, tally)
  end

  def test_unstrat
    assert_raises(Bud::CompileError) { KTest3.new }
  end

  def scratch_dir
    dir = '/tmp/' + Time.new.to_f.to_s
    Dir.mkdir(dir)
    return dir
  end

  def test_interfaces
    out_buf = StringIO.new
    prog = InterfaceAlternate.new(:stdout => out_buf)
    prov = prog.t_provides{|p| p.to_a}
    assert(prov.include? ["inc", true])
    assert(prov.include? ["stor", true])
    assert(prov.include? ["outc", false])
  end

  def test_visualization
    out_buf = StringIO.new("", "w")
    # XXX: we need an explicit port number so that we can use dbm tables, but
    # otherwise the port number is irrelevant
    program = KTest2.new(:trace => true, :port => 24321,
                         :quiet => true, :stdout => out_buf)
    `rm -r DBM_KTest2*`
    dir = scratch_dir

    program.run_bg
    write_graphs({}, program.builtin_tables, program.t_cycle, program.t_depends,
                 program.t_rules, "#{dir}/test_viz", dir, :dot, false, nil, 1, {})
    program.stop
  end

  def get_content(file)
    content = ''
    fp = File.open(file, "r")
    while (s = fp.gets)
      content += s
    end
    fp.close
    content
  end

  def str2struct(content)
    ret = {}
    content.split(";").each do |con|
      if con =~ /([^\[]+)\s*\[([^\]]+)\]/
        lhs, rhs = $1, $2
        lhs = lhs.gsub(/\s+/, "")
        ret[lhs] = {}
        rhs.split(/\s*,\s*/).each do |pair|
          k, v = pair.split(/\s*=\s*/)
          ret[lhs][k] = v
        end
      end
    end
    ret
  end

  def test_plotting
    out_buf = StringIO.new("", "w")
    program = KTest2.new(:output => :dot, :stdout => out_buf)
    program.run_bg

    dep = DepAnalysis.new
    dep.providing <+ program.t_provides.to_a
    dep.depends <+ program.t_depends.map{|d| [d.lhs, d.op, d.body, d.nm]}
    dep.sync_do

    dir = scratch_dir
    graph_from_instance(program, "#{dir}/test_graphing", dir, true, :dot)
    content = get_content("#{dir}/test_graphing.svg")

    looks = str2struct(content)

    assert_match(/upd -> "interm, mystate"\s+\[.*label=" \+\/-"/m, content)
    assert_match(/S -> upd/, content)
    assert_match(/S -> req/, content)
    assert_match(/sinkhole -> "??"/, content)
    refute_match(/upd -> "\?\?"/, content)
    refute_match(/req -> "\?\?"/, content)
    `rm -r #{dir}`
    program.stop
  end

  def test_labels
    p = Divergence.new(:output => :dot)
    p.run_bg
    p.sync_do   # XXX: why is this necessary?

    dir = scratch_dir
    graph_from_instance(p, "#{dir}/test_labels", dir, true, :dot)
    content = get_content("#{dir}/test_labels.svg")
    looks = str2struct(content)

    assert_equal("yellow", looks["serv1"]["color"])
    assert_equal("rect", looks["serv1"]["shape"])
    assert_equal("red", looks["agg"]["color"])
    assert_equal("yellow", looks["chan"]["color"])
    assert_equal("dashed", looks["cli1->chan"]["style"])
    assert_equal("\"outs\\n(D)\"", looks["outs"]["label"])
    assert_equal("\"chan\\n(A)\"", looks["chan"]["label"])

    `rm -r #{dir}`
    p.stop
  end

  def test_underspecified
    out_buf = StringIO.new("", "w")
    u = Underspecified.new(:stdout => out_buf)
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
    assert_equal(3, t.stratified_rules.length)
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
    `rm foofoo.svg`
  end

  def test_spacetime2
    inp = [
      [:ping, :a, :b, 1, 3],
      [:ping, :b, :c, 4, 11],
      [:pong, :c, :b, 12, 5],
      [:pong, :b, :a, 6, 2]
    ]
    do_spacetime(inp, "foo2")
    `rm foo2.svg`
  end

  def test_spacetime3
    inp = [
      [:ping1, :a, :b, 1, 3],
      [:ping2, :a, :b, 1, 4],
      [:ping3, :a, :b, 1, 5],
      [:ping4, :a, :b, 1, 6]
    ]
    do_spacetime(inp, "foo3")
    `rm foo3.svg`
  end

  def do_spacetime(inp, out)
    st = SpaceTime.new(inp)
    st.process
    st.finish(out)
  end
end

class TestThetaMeta < MiniTest::Unit::TestCase
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
      next if VizUtil.ma_tables.keys.include? dep.lhs.to_sym
      assert(!dep.nm, "this dependency shouldn't be marked nonmonotonic: #{dep.inspect}")
    end
  end
end

class StratBody
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
  end

  bloom do
    t1 <= t2 {|t| [t.key + 1, t.val + 1]}
    t1 <= t3.group([:key], max(:val))
    t2 <= t4 {|t| [t.key + 2, t.val + 2]}
  end
end

class TestStratProperties < MiniTest::Unit::TestCase
  def test_strat_body
    s = StratBody.new
    rule_strata = s.t_rule_stratum.map {|rs| [rs.rule_id, rs.stratum]}
    assert_equal([[0, 0], [1, 1], [2, 0]].to_set, rule_strata.to_set)
  end
end
