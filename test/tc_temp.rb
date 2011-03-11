require 'test_common'

class BasicTemp
  include Bud
  
  state do
    scratch :out, [:val]
    scratch :inski
  end
  
  declare 
  def rules
    temp(tmpy) <= inski
    out <= tmpy {|t| [t.val]}
  end
end

class DupTemp < BasicTemp
  declare
  def rules
    temp(tmpy) <= inski
    temp(tmpy) <= inski
    out <= tmpy {|t| [t.val]}
  end
end

class TempNext 
  include Bud
  state do
    scratch :inski, [:c1, :c2] => [:c3, :c4]
    scratch :wait, [:c1, :c2] => [:c3, :c4]
    scratch :out, [:c1, :c2] => [:c3, :c4]
  end
  
  declare 
  def populate
    temp(tmpy) <= inski
    temp(waity) <+ wait
    out <= tmpy
    out <= waity
  end
end

class TempNoSchema
  include Bud
  
  declare
  def logic
    temp(out) <= [[1,2], [3, 4]]
  end
end

class TestTemps < Test::Unit::TestCase
  def test_basic_temp
    p = BasicTemp.new
    p.inski <+ [[1,1],
                [2,2],
                [3,3]]
    p.tick
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.val]}.sort)
  end
  def test_retemp
    p = BasicTemp.new
    p.run_bg
    p.sync_do{p.inski <+ [[1,1],
                [2,2],
                [3,3]]}
    p.stop_bg
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.val]}.sort)
  end
  def test_temp_next
    p = TempNext.new
    p.inski <+ [[1,1,2,2],
                [2,2,3,3],
                [3,3,4,4]]
    p.wait <+  [[5,5,6,6],
                [7,7,8,8],
                [9,9,9,9]]
    p.tick
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.c1]}.sort)
    p.tick
    assert_equal(3, p.out.length)
    assert_equal([[5], [7], [9]], p.out.map{|o| [o.c1]}.sort)
  end
  def test_dup_tmp
    assert_raise(Bud::BudError) {DupTemp.new}
  end
  def test_no_schema
    p = TempNoSchema.new
    assert_raise(Bud::BudError) {p.tick}
  end
end
