require 'rubygems'
require 'bud'
require 'test/unit'

class TickleCount < Bud
  def state
    channel :loopback, ['cnt']
    channel :mcast, ['@addr', 'cnt']
    table   :result, ['nums']
    table   :mresult, ['nums']
  end

  def bootstrap
    loopback <~ [[0]]
  end

  declare
  def count_to_5
    loopback <~ loopback.map{|l| [l.cnt + 1] if l.cnt < 6 }
    result <= loopback.map{|l| [l.cnt] if l.cnt == 5}
    mcast <~ loopback.map{|l| [@ip_port, l.cnt] if l.cnt < 6}
    mresult <= mcast.map{|m| [m.cnt] if m.cnt == 5}
  end
end

class TestTickle < Test::Unit::TestCase
  def test_tickle_count
    c = TickleCount.new
    c.run_bg
    sleep 1
    c.stop_bg
    assert_equal("[[5]]", c.result.map{|t| t}.inspect)
    assert_equal("[[5]]", c.mresult.map{|t| t}.inspect)
  end
end
