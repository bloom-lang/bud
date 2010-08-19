class ShortestPaths < Bud
  def initialize(ip, port)
    super(ip,port)
  end
  
  def state
    table :link, ['from', 'to', 'cost']
    table :path, ['from', 'to', 'next', 'cost']
    table :shortest, ['from', 'to'], ['next', 'cost']
    table :minz,['cost']
    table :minmaxsumcntavg, ['from', 'to'], ['mincost', 'maxcost', 'sumcost', 'cnt', 'avgcost']
  end
  
  def declaration
    strata[0] = rules {

      path <= link.map{|e| [e.from, e.to, e.to, e.cost]}

      j = join [link, path], [path.from, link.to]
      path <= j.map do |l,p|
        [l.from, p.to, p.from, l.cost+p.cost] # if l.to == p.from
      end

      shortest <= path.argagg(:min, [path.from, path.to], path.cost)
      minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

      minz <= shortest.group(nil, min(shortest.cost))
    }
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = ShortestPaths.new('localhost', 12345)
    assert_equal(0, program.strata.length)
    assert_nothing_raised( RuntimeError) { program.tick }
    assert_equal(3, program.strata.length)
  end
  
end
