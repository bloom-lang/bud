require 'rubygems'
require 'bud'
require 'test/unit'

# XXX: these tests are somewhat bogus because channels use UDP to send messages,
# so we can't count on messages being delivered.

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
    loopback <~ loopback.map{|l| [l.cnt + 1] if l.cnt < 6}
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

class RingMember < Bud
  def state
    channel :pipe, ['@addr', 'cnt']
    scratch :kickoff, ['cnt']
    table :next_guy, ['addr']
    table :last_cnt, ['cnt']
    periodic :tik, 1
  end

  declare
  def ring_msg
    stdio <~ pipe.map {|p| ["Self: " + @ip_port + ", cnt = " + p.cnt.to_s]}
    stdio <~ kickoff.map {|k| ["KICKOFF: " + k.cnt.to_s]}
    pipe <~ kickoff.map {|k| [@ip_port, k.cnt.to_i]}
    pipe <~ join([pipe, next_guy]).map {|p,n| [n.addr, p.cnt.to_i + 1] if p.cnt.to_i < 10}
  end

  declare
  def update_log
    last_cnt <+ pipe.map {|p| [p.cnt]}
    last_cnt <- join([pipe, last_cnt]).map {|p, lc| [lc.cnt]}
  end
end

class TestRing < Test::Unit::TestCase
  RING_SIZE = 4

  def test_basic
    return
    ring = []
    0.upto(RING_SIZE - 1) do |i|
      ring[i] = RingMember.new
      ring[i].run_bg
    end

    ring.each_with_index do |r, i|
      next_idx = i + 1
      next_idx = 0 if next_idx == RING_SIZE
      next_addr = ring[next_idx].ip_port

      r.async_do {
        r.next_guy << [next_addr]
      }
    end

    first = ring.first
    first.async_do {
      first.kickoff <+ [[0]]
    }

    sleep 5

    ring.each_with_index do |r, i|
      r.async_do {}
      r.stop_bg
      puts "#{i}: last_cnt = #{r.last_cnt.to_a.inspect}, len = #{r.last_cnt.length}"
    end
  end
end
