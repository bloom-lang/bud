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
    assert_equal([[5]], c.result.to_a)
    assert_equal([[5]], c.mresult.to_a)
  end
end

class RingMember < Bud
  def state
    channel :pipe, ['@addr', 'cnt']
    scratch :kickoff, ['cnt']
    table :next_guy, ['addr']
    table :last_cnt, ['cnt']
  end

  declare
  def ring_msg
    pipe <~ kickoff.map {|k| [@ip_port, k.cnt.to_i]}
    pipe <~ join([pipe, next_guy]).map {|p,n| [n.addr, p.cnt.to_i + 1] if p.cnt.to_i < 39}
  end

  declare
  def update_log
    last_cnt <+ pipe.map {|p| [p.cnt]}
    last_cnt <- join([pipe, last_cnt]).map {|p, lc| [lc.cnt]}
  end
end

class TestRing < Test::Unit::TestCase
  RING_SIZE = 10

  def test_basic
    ring = []
    0.upto(RING_SIZE - 1) do |i|
      ring[i] = RingMember.new
      ring[i].run_bg
    end

    ring.each_with_index do |r, i|
      next_idx = i + 1
      next_idx = 0 if next_idx == RING_SIZE
      next_addr = ring[next_idx].ip_port

      r.sync_do {
        r.next_guy << [next_addr]
      }
    end

    first = ring.first
    first.async_do {
      first.kickoff <+ [[0]]
    }

    sleep 3

    ring.each_with_index do |r, i|
      # XXX: we need to do a final tick here to ensure that each Bud instance
      # applies pending <+ and <- derivations. See issue #50.
      r.sync_do {}
      r.stop_bg
      assert_equal(r.last_cnt.first, [30 + i])
    end
  end
end
