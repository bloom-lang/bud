class CombosBud < Bud

  def state
    table :r, ['x', 'y']
    table :s_tab, ['x', 'y']
    table :t, ['x', 'y']
    scratch :simple_out, ['x', 'y1', 'y2']
    scratch :match_out, ['x', 'y1', 'y2']
    scratch :chain_out, ['x1', 'x2', 'x3', 'y1', 'y2', 'y3']
    scratch :flip_out, ['x1', 'x2', 'x3', 'y1', 'y2', 'y3']
    scratch :nat_out, ['x1', 'x2', 'x3', 'y1', 'y2', 'y3']
  end
  
  def declaration
    strata[0] = rules {
      r << ['a', 1]
      r << ['b', 1]
      r << ['b', 2]
      r << ['c', 1]
      r << ['c', 2]
      s_tab << ['a', 1]
      s_tab << ['b', 2]
      s_tab << ['c', 1]
      s_tab << ['c', 2]
      t << ['a', 1]
      t << ['z', 1]

      j = join [r,s_tab], [r.x, s_tab.x]
      simple_out <= j.map { |t1,t2| [t1.x, t1.y, t2.y] }
      
      k = join [r,s_tab], [r.x, s_tab.x], [r.y, s_tab.y]
      match_out <= k.map { |t1,t2| [t1.x, t1.y, t2.y] }
      
      l = join [r,s_tab,t], [r.x, s_tab.x], [s_tab.x, t.x]
      chain_out <= l.map { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y, t2.y, t3.y] }
      
      m = join [r,s_tab,t], [r.x, s_tab.x, t.x]
      flip_out <= m.map { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y, t2.y, t3.y] }
      
      n = natjoin [r,s_tab,t]
      nat_out <= m.map { |t1, t2, t3| [t1.x, t2.x, t3.x, t1.y, t2.y, t3.y] }
    }
  end
end

class TestJoins < Test::Unit::TestCase
  def test_combos
    program = CombosBud.new('localhost', 12345)
    assert_nothing_raised( RuntimeError) { program.tick }
    simple_outs = program.simple_out.map {|t| t}
    assert_equal(7, simple_outs.length)
    assert_equal(1, simple_outs.select { |t| t[0] == 'a'} .length)
    assert_equal(2, simple_outs.select { |t| t[0] == 'b'} .length)
    assert_equal(4, simple_outs.select { |t| t[0] == 'c'} .length)
  end
  
  def test_secondary_join_predicates
    program = CombosBud.new('localhost', 12345)
    assert_nothing_raised( RuntimeError) { program.tick }
    match_outs = program.match_out.map {|t| t}
    assert_equal(4, match_outs.length)
    assert_equal(1, match_outs.select { |t| t[0] == 'a'} .length)
    assert_equal(1, match_outs.select { |t| t[0] == 'b'} .length)
    assert_equal(2, match_outs.select { |t| t[0] == 'c'} .length)
  end   
  
  def test_3_joins
    program = CombosBud.new('localhost', 12345)
    assert_nothing_raised( RuntimeError) { program.tick }
    chain_outs = program.chain_out.map {|t| t}
    assert_equal(1, chain_outs.length)
    flip_outs = program.flip_out.map {|t| t}
    assert_equal(1, flip_outs.length)
    nat_outs = program.nat_out.map{|t| t}
    assert_equal(1, nat_outs.length)
    assert_equal(chain_outs, flip_outs)
  end
end