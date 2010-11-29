require 'rubygems'
require 'bud'
require 'test/unit'

class ShortestPaths < Bud
  def initialize(ip, port)
    super(ip,port)
  end
  
  def state
    table :link, ['from', 'to', 'cost']
    table :path, ['from', 'to', 'next', 'cost']
    table :shortest, ['from', 'to'], ['next', 'cost']
    table :minmaxsumcntavg, ['from', 'to'], ['mincost', 'maxcost', 'sumcost', 'cnt', 'avgcost']
    table :avrg, ['from', 'to'], ['ave', 'some', 'kount']
  end

  def bootstrap
    link << ['a', 'b', 1]
    link << ['a', 'b', 4]
    link << ['b', 'c', 1]
    link << ['c', 'd', 1]
    link << ['d', 'e', 1]
  end
  
  declare
  def program
    path <= link.map{|e| [e.from, e.to, e.to, e.cost]}

    j = join [link, path], [path.from, link.to]
    path <= j.map do |l,p|
      [l.from, p.to, p.from, l.cost+p.cost] # if l.to == p.from
    end

    # second stratum
    shortest <= path.argmin([path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), max(path.cost), sum(path.cost), count, avg(path.cost))
    avrg <= path.group([:from, :to], min(:cost), max(:cost), sum(:cost), count, avg(:cost)) do |t|
      [t[0], t[1], t[6], t[4], t[5]]
    end
  end
end

class PriorityQ < Bud
  def initialize(ip, port)
    super(ip,port)
  end
  
  def state
    table :q, ['item'], ['priority']
    scratch :out, ['item'], ['priority']
    scratch :minny, ['priority']
    scratch :out2, ['item'], ['priority']
  end
  
  def bootstrap
    q << ['c', 2]
    q << ['d', 3]
    q << ['a', 1]
    q << ['b', 2]
  end

  declare
  def program

    # second stratum
    out <= q.argagg(:min, [], q.priority)
    minny <= q.group(nil, min(q.priority))
    q <- out.map{|t| t}

    # third stratum
    out2 <= natjoin([q,minny]).map{|q, m| q}
  end
end

class DupAggs < Bud
  def state
    table :tab, ['i']
#    scratch :out, ['s1', 's2']
  end
  
  def bootstrap
    tab << [1]
    tab << [2]
  end
  
  declare
  def prog
    out = tab.group(nil,sum(tab.i), sum(tab.i))
    p out.inspect
  end
end

class TestAggs < Test::Unit::TestCase
  def test_paths
    program = ShortestPaths.new('localhost', 12345)
    assert_nothing_raised( RuntimeError) { program.tick }
  
    program.minmaxsumcntavg.each do |t|
      assert(t[4])
      assert(t[2] <= t[3])
      assert_equal(t[4]*1.0 / t[5], t[6])
    end
    program.avrg.each do |t|
      assert_equal(t.some*1.0 / t.kount, t.ave)
    end
    program.shortest.each do |t|
      assert_equal(t[1][0] - t[0][0], t[3])
    end
    shorts = program.shortest.map {|s| [s.from, s.to, s.cost]}
    costs = program.minmaxsumcntavg.map {|c| [c.from, c.to, c.mincost]}
    assert_equal([], shorts - costs)
  end
  
  def test_dup_aggs
  end
  
  def test_non_exemplary
    program = ShortestPaths.new('localhost', 12345)
    assert_nothing_raised( RuntimeError) { program.tick }
    assert_raise(Bud::BudError) {p = program.path.argagg(:count, [program.path.from, program.path.to], nil)}
    assert_raise(Bud::BudError) {p = program.path.argagg(:sum, [program.path.from, program.path.to], program.path.cost)}
    assert_raise(Bud::BudError) {p = program.path.argagg(:avg, [program.path.from, program.path.to], program.path.cost)}
  end
  
  def test_argaggs
    program = PriorityQ.new('localhost', 12345)
    assert_nothing_raised (RuntimeError) { program.tick }
    argouts = program.out.map{|t| t}
    basicouts = program.out2.map{|t| t}
    assert_equal([], argouts - basicouts)
  end
end
