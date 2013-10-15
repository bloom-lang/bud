require './test_common'
require 'stringio'

class MetricsTest
  include Bud
  
  state do
    table :t1
    scratch :s1
  end
  
  bloom do
    s1 <= t1
  end
end


class TestMetrics < MiniTest::Unit::TestCase
  def test_metrics
    out, err = capture_io do
      p = MetricsTest.new(:metrics => true)
      5.times {
        p.sync_do { p.t1 <+ [[p.budtime, 5]] }
      }
      p.stop
    end

    assert(out.include? %Q{"","count","5"})
  end
end
