require './test_common'

class TestSort < MiniTest::Unit::TestCase
  class SortDelay
    include Bud

    state do
      interface input, :in_t, [:payload]
      interface output, :out_t, [:ident] => [:payload]
      scratch  :delaybuf, [:ident] => [:payload]
    end

    bloom do
      delaybuf <= in_t.sort.each_with_index.map {|a, i| [i, a] }
      out_t <= delaybuf
    end
  end

  class SortTuples
    include Bud
    state do
      scratch :tab, [:a, :b]
      scratch :out, [:i] => [:a, :b]
    end
    bloom do
      out <= tab.sort{|t1,t2| t1.a == t2.a ? t1.b <=> t2.b : t1.a <=> t2.a}.each_with_index{|tup, i| [i, tup.a, tup.b]}
    end
  end

  def test_sort_simple
    p = SortTuples.new
    p.tab <+ [
        [20, 20],
        [20, 30],
        [1,  5],
        [1,  10]
    ]
    p.tick
    out = p.out.map{|t| t.to_a}
    assert_equal([[0, 1, 5], [1, 1, 10], [2, 20, 20], [3, 20, 30]], out.sort{|a,b| a[0] <=> b[0]})
  end

  def test_sort_pro
    p = SortDelay.new
    p.run_bg
    r = p.sync_callback(:in_t, [[5], [1], [100], [6]], :out_t)
    assert_equal([[0, [1]], [1, [5]], [2, [6]], [3, [100]]], r.to_a.sort)
    p.stop
  end

  class SortRescan
    include Bud

    state do
      table :t1
      scratch :s1
      scratch :sort_res
    end

    bloom do
      sort_res <= t1.sort
      sort_res <= s1
    end
  end

  def test_sort_rescan
    i = SortRescan.new
    i.t1 <+ [[3, 4], [5, 10]]
    i.tick
    assert_equal([[3, 4], [5, 10]], i.sort_res.to_a.sort)
    i.tick
    assert_equal([[3, 4], [5, 10]], i.sort_res.to_a.sort)
    i.s1 <+ [[1, 1]]
    i.t1 <+ [[9, 9]]
    i.tick
    assert_equal([[1, 1], [3, 4], [5, 10], [9, 9]], i.sort_res.to_a.sort)
  end
end
