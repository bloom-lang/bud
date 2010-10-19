require 'rubygems'
require 'bud'
require 'test/unit'

class TemporalBudTest < Bud
  def state
    periodic :tik, 1
    table :log, ['time', 'too']
  end

  declare 
  def logic
    log <= tik 
  end
end


class TestTimer < Test::Unit::TestCase
  def test_timer
    t = TemporalBudTest.new("localhost", 17890)
    assert_nothing_raised(RuntimeError) { t.run_bg }
    sleep 5
    assert_operator(3, :<, t.log.length)
    assert_operator(7, :>, t.log.length)
  end
end
