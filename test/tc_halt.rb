require './test_common'

class Halts
  include Bud

  state do
    scratch :tbl, [:key]
    periodic :timer, 0.01
  end

  bootstrap do
    tbl << [1]
  end

  bloom do
    halt <= tbl {|t| t if t.key == 2}
    tbl <+ tbl {|t| [t.key+1]}
  end
end

class TestHalt < MiniTest::Unit::TestCase
  def test_halt
    program = Halts.new
    program.run_bg
    assert_raises(Bud::ShutdownWithCallbacksError) {4.times{program.delta(:tbl)}}
  end

  def test_halt_fg
    run_fg_finished = false
    t = Thread.new do
      program = Halts.new
      program.run_fg
      run_fg_finished = true
    end
    t.join
    assert(run_fg_finished)
  end
end
