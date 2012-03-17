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
    sio = StringIO.new
    begin
      old_stdout, $stdout = $stdout, sio
      p = MetricsTest.new(:metrics => true, :port => 56789)
      5.times {
        p.sync_do { p.t1 <+ [[p.budtime, 5]] }
      }
      p.stop
    ensure
      $stdout = old_stdout
    end
  end
end
