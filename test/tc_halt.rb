# simple word count
require 'test_common'
require 'timeout'

class Exits
  include Bud

  state do
    scratch :tbl, [:key]
    periodic :timer, 0.01
  end

  bootstrap do
    tbl << [1]
  end
  bloom do
    halt <= tbl{|t| t if t.key == 2}
    tbl <+ tbl{|t| [t.key+1]}
  end
end

class TestExit < Test::Unit::TestCase
  def test_exit
    program = Exits.new
    program.run_bg
    assert_raise(Bud::BudShutdownWithCallbacksError) {4.times{program.delta(:tbl)}}
  end
end
