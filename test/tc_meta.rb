require 'rubygems'
require 'bud'
require 'test/unit'
require 'digest/md5'


class LocalShortestPaths < Bud
  def state
    table :link, ['from', 'to', 'cost']
    table :link2, ['from', 'to', 'cost']
    table :empty, ['id']
    table :path, ['from', 'to', 'next', 'cost']
    table :shortest, ['from', 'to'], ['next', 'cost']
    table :minz,['cost']
    table :minmaxsumcntavg, ['from', 'to'], ['mincost', 'maxcost', 'sumcost', 'cnt', 'avgcost']
  end

  declare 
  def program
    link2 <= link.map{|l| l unless empty.include? l.id } 
    path <= link2.map{|e| [e.from, e.to, e.to, e.cost]}
    path <= join([link2, path]).map do |l, p|
      [l.from, p.to, p.from, l.cost+p.cost] if l.to == p.from
    end

    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

    minz <= shortest.group(nil, min(shortest.cost))
  end
end

class KTest < Bud
  def state
    interface input, :upd, ['data']
    interface input, :req, ['id']
    interface output, :resp, ['id', 'data']
    table :mystate, ['data']

    interface output, :qq, ['data']
  
  end

  declare
  def update
    mystate <+ upd
    mystate <- join([upd, mystate]).map{|i, s| s }
  end
  
  declare
  def respond
    resp <= join([req, mystate]).map{|r, s| [r.id, s.data] } 
  end
end

class KTest2 < KTest
  declare
  def update
    mystate <= upd
    mystate <- join([upd, mystate]).map{|i, s| s }
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new('localhost', 134634)
    assert_equal(0, program.strata.length)
    assert_nothing_raised( RuntimeError) { program.tick }
    assert_equal(4, program.strata.length)
  end

  def test_visualization
    program = KTest.new('localhost', 34521, {'dump' => true, 'visualize' => true, 'enforce_rewrite' => true, 'provenance' =>true})

  dep = DepAnalysis.new("localhost", 23525)
  
  program.strat_state.depends_tc.each{|d| dep.depends_tc << d }
  program.strat_state.cycle.each{|d| dep.cycle << d }
  program.provides.each{|p| puts "provide " + p.inspect; dep.providing << p }
  program.demands.each{|d| puts "demand " + d.inspect; dep.demanding << d }

  dep.tick


    md5 = Digest::MD5.hexdigest(File.read("LocalShortestPaths_gvoutput.pdf"))
    #assert_equal("06cd9cc947cfeb7f038ea1b8f6b75fd2", md5)
  end
  
end
