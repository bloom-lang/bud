require 'rubygems'
require 'bud'
require 'test/unit'

class TemporalBudTest < Bud
  def state
    periodic :tik, 0.1
    table :log, ['ident', 'time']
  end

  declare 
  def logic
    log <= tik 
  end
end


class TestTimer < Test::Unit::TestCase
  def test_timer
    t = TemporalBudTest.new("localhost", 17890)
    t.tick
    assert_nothing_raised(RuntimeError) { t.run_bg }
    sleep 3
    assert_operator(2, :<, t.log.length)
    assert_operator(7, :>, t.log.length)
  end
end
