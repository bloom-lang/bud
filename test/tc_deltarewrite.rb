require 'rubygems'
require 'bud'
require 'test/unit'

class Paths < Bud
  attr_reader :cnt, :pcnt
  def initialize(ip, port)
    @cnt = @pcnt = 0
    super(ip,port)
  end
  
  def state
    table :link, ['from', 'to']
    table :path, ['from', 'to']
  end
  
  declare
  def program
    path <= link.map{|e| (@cnt = @cnt + 1) and [e.from, e.to] }

    j = join [link, path], [path.from, link.to]
    path <= j.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end
  end
end


class SP2 < Paths
  attr_reader :m1cnt, :m2cnt
  def initialize(ip, port)
    @m1cnt = @m2cnt = 0
    super(ip, port)
  end

  def state
    super
    scratch :d_link, ['from', 'to']
    scratch :d_path, ['from', 'to']

    table :n_link, ['from', 'to']
    table :n_path, ['from', 'to']
  end

  declare
  def program
    d_path <+ d_link.map{|e| (@cnt = @cnt + 1) and [e.from, e.to]}

    j = join [d_link, n_path], [n_path.from, d_link.to]
    j2 = join [n_link, d_path], [d_path.from, n_link.to]

    d_path <+ j.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to] 
    end

    d_path <+ j2.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end
  end

  declare
  def next_strat
    n_path <+ d_path.map{|p| (@m1cnt = @m1cnt + 1) and p}
    n_link <+ d_link.map{|l| (@m2cnt = @m2cnt + 1) and l}
  end

  declare 
  def alfinal
    link <+ n_link.map{|n| n if d_link.empty? and d_path.empty?}
    path <+ n_path.map{|n| n if d_link.empty? and d_path.empty?}

    # need to clean up the intermediates
    n_link <+ n_link.map{|n| n if d_link.empty? and d_path.empty?}
    n_path <+ n_path.map{|n| n if d_link.empty? and d_path.empty?}

  end
end



class TestDelta < Test::Unit::TestCase
  def inserts(tab)
    # observe: 
    # 5 vertices. 4 edges.
    # diameter 4.  so we'll need 4 rounds (am I off by 1?) of semi-naive.
    tab <+ [['a', 'b']]  
    tab <+ [['b', 'c']] 
    tab <+ [['c', 'd']] 
    tab <+ [['d', 'e']] 

    # round 1: 5 derivations.
    # round 2: (a,c), (b,d), (c, e)
    # round 3: (a, d), (b, e)
    # round 4: (a, e)

    # total: 10 paths, of which 4 were 'base'.
    
  end

  def test_dr
    program = Paths.new('localhost', 12345)

    # PAA -- note that I still need to tick it to make link appear
    assert_nothing_raised( RuntimeError) { program.tick }
    inserts(program.link)
    assert_nothing_raised( RuntimeError) { program.tick }
    # cnt: base case applications. 
    # pcnt: inductive case applications
    assert_equal(20, program.cnt)
    assert_equal(20, program.pcnt)

    # 10-4, good buddy.
    assert_equal(10, program.path.length)
    assert_equal(4, program.link.length)
    
  end

  def test_handwrite
    program = SP2.new('localhost', 12345)
    # PAA -- note that I still need to tick it to make link appear
    assert_nothing_raised( RuntimeError) { program.tick }
    inserts(program.d_link)
    assert_nothing_raised( RuntimeError) { program.tick }
    # base case applications
    assert_equal(4, program.cnt)
    assert_nothing_raised( RuntimeError) { program.tick }
    # (is quiesent)
    assert_equal(4, program.cnt)

    # I am "seminaive"
    iters = 0
    while (program.d_link.length > 0 or program.d_path.length > 0) do  
      assert_nothing_raised( RuntimeError) { program.tick }
      iters = iters + 1
      # intermediate results are not visible.
      assert_equal(0, program.path.length)
    end

    # 4 iterations were required to process a graph w/ diameter 4
    assert_equal(4, iters)
   
    # m1cnt: # of times a d_link tuple was 'stored' in link
    # m2cnt: # of times a d_path tuple was 'stored' in path
    assert_equal(10, program.m1cnt)
    assert_equal(4, program.m2cnt)
    # and in fact these counts are final (though we need one more tick to finalize 
    # the visible predicates):
    assert_nothing_raised( RuntimeError) { program.tick }

    assert_equal(10, program.m1cnt)
    assert_equal(4, program.m2cnt)


    # same result as "naive": 10-4
    assert_equal(10, program.path.length)
    assert_equal(4, program.link.length)

  end
end
