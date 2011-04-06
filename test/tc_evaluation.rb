require 'test_common'

class Paths
  include Bud

  # a naive transitive closure of an edge relation.
  attr_reader :cnt, :pcnt
  def initialize
    @cnt = @pcnt = 0
    super()
  end

  state do
    table :link, ['from', 'to']
    table :path, ['from', 'to']
  end

  bloom :program do
    # this is the program a user might write.
    path <= link.map{|e| (@cnt = @cnt + 1) and [e.from, e.to] }
    path <= (link * path).pairs(:to => :from) do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end
  end
end


class PathsDelta < Paths
  # the same transitive closure, made seminaive with supplemental
  # rules and relations
  attr_reader :m1cnt, :m2cnt
  def initialize
    @m1cnt = @m2cnt = 0
    super()
  end

  state do
    scratch :d_link, ['from', 'to']
    scratch :d_path, ['from', 'to']
  end

  bloom :program do
    # this is the delta-rewritten program.  it is a shame that we need
    # to duplicate both the join assignment statement and the rule that
    # uses the join.
    d_path <+ d_link.map{|e| (@cnt = @cnt + 1) and [e.from, e.to]}

    temp :k <= join [d_link, path], [path.from, d_link.to]
    temp :k2 <= join [link, d_path], [d_path.from, link.to]

    d_path <+ k.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end

    d_path <+ k2.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end
  end

  bloom :next_strat do
    path <+ d_path.map{|p| (@m1cnt = @m1cnt + 1) and p}
    link <+ d_link.map{|l| (@m2cnt = @m2cnt + 1) and l}
  end
end

class PathsDeltaIndirected < PathsDelta
  state do
    table :n_link, ['from', 'to']
    table :n_path, ['from', 'to']
  end

  bloom :program do
    d_path <+ d_link.map{|e| (@cnt = @cnt + 1) and [e.from, e.to]}

    temp :k <= join [d_link, n_path], [n_path.from, d_link.to]
    temp :k2 <= join [n_link, d_path], [d_path.from, n_link.to]

    d_path <+ k.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end

    d_path <+ k2.map do |l,p|
      (@pcnt = @pcnt + 1) and [l.from, p.to]
    end
  end

  bloom :next_strat do
    n_path <+ d_path.map{|p| (@m1cnt = @m1cnt + 1) and p}
    n_link <+ d_link.map{|l| (@m2cnt = @m2cnt + 1) and l}
  end

  bloom :alfinal do
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

  def test_naive
    program = Paths.new
    inserts(program.link)
    assert_nothing_raised( RuntimeError) { program.tick }
    # cnt: base case applications.
    # pcnt: inductive case applications
    assert_equal(20, program.cnt)
    assert_equal(20, program.pcnt)

    # 10-4, good buddy. (10 paths, 4 base)
    assert_equal(10, program.path.length)
    assert_equal(4, program.link.length)
  end

  def top_half(program)
    inserts(program.d_link)
    assert_nothing_raised( RuntimeError) { program.tick }
    # base case applications
    #assert_equal(4, program.cnt)
    assert_nothing_raised( RuntimeError) { program.tick }
    # (is quiesent)
    #assert_equal(4, program.cnt)
  end

  def bottom_half(program)
    # m1cnt: # of times a d_link tuple was 'stored' in link
    # m2cnt: # of times a d_path tuple was 'stored' in path
    # 40 times? huh?
    assert_equal(10, program.m1cnt)
    assert_equal(4, program.m2cnt)
    # and in fact these counts are final:
    assert_nothing_raised( RuntimeError) { program.tick }
    assert_equal(10, program.m1cnt)
    assert_equal(4, program.m2cnt)


    # same result as "naive": 10-4
    assert_equal(10, program.path.length)
    assert_equal(4, program.link.length)
  end

  def test_handwrite_delta
    program = PathsDelta.new
    top_half(program)
    # I am "seminaive"
    iters = 0
    while (program.d_link.length > 0 or program.d_path.length > 0) do
      assert_nothing_raised( RuntimeError) { program.tick }
      iters += 1
      # however, intermediate results are visible at each step of iteration:
      case iters
        when 1 then assert_equal(program.path.length, 4)
        when 2 then assert_equal(program.path.length, 7)
        when 3 then assert_equal(program.path.length, 9)
        when 4 then assert_equal(program.path.length, 10)
      end
    end
    # 4 iterations were required to process a graph w/ diameter 4
    assert_equal(4, iters)
    bottom_half(program)
  end

  def test_handwrite_indirect_delta
    program = PathsDeltaIndirected.new
    top_half(program)
    # I am "seminaive"
    iters = 0
    while (program.d_link.length > 0 or program.d_path.length > 0) do
      assert_nothing_raised( RuntimeError) { program.tick }
      iters += 1
      # intermediate results are not visible.
      assert_equal(0, program.path.length)
    end
    # 4 iterations were required to process a graph w/ diameter 4
    assert_equal(4, iters)
    bottom_half(program)
  end
end
