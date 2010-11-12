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
    #link2 <= link.group([link.from, link.to], count(link.cost))
    path <= link2.map{|e| [e.from, e.to, e.to, e.cost]}

    #j = join [link2, path], [path.from, link2.to]
    #path <= j.map do |l,p|
    #path <= join([link2, path], [path.from, link2.to]).map do |l, p|
    path <= join([link2, path]).map do |l, p|
      [l.from, p.to, p.from, l.cost+p.cost] if l.to == p.from
    end

    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    minmaxsumcntavg <= path.group([path.from, path.to], min(path.cost), min(path.cost), sum(path.cost), count, avg(path.cost))

    minz <= shortest.group(nil, min(shortest.cost))
  end
end

class TestMeta < Test::Unit::TestCase
  def test_paths
    program = LocalShortestPaths.new('localhost', 12345)
    assert_equal(0, program.strata.length)
    assert_nothing_raised( RuntimeError) { program.tick }
    assert_equal(4, program.strata.length)
  end

  def ntest_visualization
    assert_nothing_raised(RuntimeError) { program = LocalShortestPaths.new('localhost', 34521, {'dump' => true, 'visualize' => true}) }
    md5 = Digest::MD5.hexdigest(File.read("LocalShortestPaths_gvoutput.pdf"))
    assert_equal("06cd9cc947cfeb7f038ea1b8f6b75fd2", md5)
  end
  
end
