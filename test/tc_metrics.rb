require 'test_common'
require 'stringio'

class MetricsTest
  include Bud
  
  state do
    table :t1
    scratch :s1
    periodic :go, 0.2
  end
  
  bootstrap do
    t1 << [1,1]
  end
  bloom do
    s1 <= t1
  end
end


class TestMetrics < Test::Unit::TestCase
  def test_metrics
    sio = StringIO.new
    old_stdout, $stdout = $stdout, sio
    p = MetricsTest.new(:metrics => true, :port => 56789)
    p.run_bg
    sleep 1
    p.stop_bg
    $stdout = old_stdout
  end
end
