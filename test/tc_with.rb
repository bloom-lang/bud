require 'test_common'

class BasicWith
  include Bud
  state do
    table :out, [:val]
    scratch :inski
    scratch :tmpy, [:val]
  end
  bootstrap {inski <= [[1,2]]; tmpy <= [[:a],[:b]]}
  bloom :rules do
    with :tmpy <= inski {|i| i}, begin
      out <= tmpy {|t| [t.val]}
      out <= tmpy {|t| [t.key]}
    end
  end
end


module WithMod
  state do
    table :out, [:val]
    scratch :inski
    scratch :tmpy, [:val]
  end
  
  bootstrap {inski <= [[1,2]]; tmpy <= [[:a],[:b]]}

  bloom :rules do
    with :tmpy <= inski, begin
      out <= tmpy {|t| [t.val]}
      out <= tmpy {|t| [t.key]}
    end
    with :tmpy <= inski {|i| [i.key*10,i.val*10]}, begin
      out <= tmpy {|t| [t[1]]}
      out <= tmpy {|t| [t[0]]}
    end
  end
end

class With
  include Bud
  import WithMod => :b
  
  state do 
    scratch :out, [:val]
  end
  
  bloom :xfer do
    out <= b.out
  end
end

class TestWith < Test::Unit::TestCase
  def test_basic
    c = BasicWith.new
    c.tick
    assert_equal([[1],[2]], c.out.to_a.sort)
  end
  def test_nested_fancy
    c = With.new
    c.tick
    assert_equal([[1],[2],[10],[20]], c.out.to_a.sort)
  end
end
