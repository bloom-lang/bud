require './test_common'

class TestSort < Test::Unit::TestCase
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

  def test_sort_pro
    p = SortDelay.new
    p.run_bg
    r = p.sync_callback(:in_t, [[5], [1], [100], [6]], :out_t)
    assert_equal([[0, [1]], [1, [5]], [2, [6]], [3, [100]]], r.to_a.sort)
    p.stop_bg
  end
end

