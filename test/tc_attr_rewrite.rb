require './test_common'

class TestCols < MiniTest::Unit::TestCase
  class SimpleCols
    include Bud
    state do
      table :t1
      table :t2
    end

    bootstrap do
      t1 << [1,2]
    end
    
    bloom do
      t2 <= t1 {|t| [t.key, t.val]}
    end
  end

  def test_simple_cols
    program = SimpleCols.new
    program.tick
    assert_equal([[1,2]], program.t2.to_a)
  end
  
  class NestedCols
    include Bud
    state do
      table :t1
      table :t2
      table :t3
    end
    bootstrap do
      t1 << [1,2]
      t3 << [1,3]
    end

    bloom do
      t2 <= t1 {|t| [t.key, t.val] if t3.each{|x| t.key == x.key}}
    end
  end  
  
  def test_nested_cols
    program = NestedCols.new
    program.tick
    assert_equal([[1,2]], program.t2.to_a)
  end
  
  class BadNestedCols
    include Bud
    state do
      table :t1
      table :t2
      table :t3, [:val, :key]
    end
    bootstrap do
      t1 << [1,2]
      t3 << [1,3]
    end

    bloom do
      t2 <= t1 {|t| [t.key, t.val] if t3.each{|t| t.key == t.val}}
    end
  end  
  
  def test_bad_nested_cols
    assert_raises(Bud::CompileError) {BadNestedCols.new}
  end
end
