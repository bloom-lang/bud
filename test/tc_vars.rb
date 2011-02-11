# variable design still rather tentative
require 'test_common'

class VarBud < Bud
  def state
    var :x
    tmpvar :y
    table :tbl, [:k1, :k2] => [:v1, :v2]
  end

  declare
  def program
    self.x = 4
    self.y = 5
  end
end

class VarBudDup < Bud
  def state
    var :x
    tmpvar :t
    table :tbl, [:k1, :k2] => [:v1, :v2]
  end

  declare
  def program
    self.x = 4
    self.x = 5
  end
end

class TestVars < Test::Unit::TestCase
  def test_goodvars
    program = VarBud.new
    assert_nothing_raised(RuntimeError) { program.tick }
    assert_nothing_raised(RuntimeError) { program.tick }
  end

  def test_dupvars
    program = VarBudDup.new
    assert_raise(Bud::KeyConstraintError) {program.tick}
  end
end
