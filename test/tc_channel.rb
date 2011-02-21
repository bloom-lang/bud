require 'test_common'

class TickleCount
  include Bud

  state {
    channel :loopback, [:cnt]
    channel :mcast, [:@addr, :cnt]
    table   :result, [:nums]
    table   :mresult, [:nums]
  }

  bootstrap do
    loopback <~ [[0]]
  end

  declare
  def count_to_5
    loopback <~ loopback.map{|l| [l.cnt + 1] if l.cnt < 6}
    result <= loopback.map{|l| [l.cnt] if l.cnt == 5}

    mcast <~ loopback.map{|l| [ip_port, l.cnt] if l.cnt < 6}
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

class RingMember
  include Bud

  state {
    channel :pipe, [:@addr, :cnt]
    scratch :kickoff, [:cnt]
    table :next_guy, [:addr]
    table :last_cnt, [:cnt]
  }

  declare
  def ring_msg
    pipe <~ kickoff.map {|k| [ip_port, k.cnt.to_i]}
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
      r.sync_do
      r.stop_bg
      assert_equal([30 + i], r.last_cnt.first)
    end
  end
end

class ChannelWithKey
  include Bud

  state {
    channel :c, [:@addr, :k1] => [:v1]
    scratch :kickoff, [:addr, :v1, :v2]
    table :recv, c.key_cols => c.cols
  }

  declare
  def send_msg
    c <~ kickoff.map {|k| [k.addr, k.v1, k.v2]}
    recv <= c
  end
end

class TestChannelWithKey < Test::Unit::TestCase
  def test_basic
    p1 = ChannelWithKey.new
    p2 = ChannelWithKey.new

    p1.run_bg
    p2.run_bg

    target_addr = p2.ip_port
    p1.sync_do {
      p1.kickoff <+ [[target_addr, 10, 20]]
      # Test that directly inserting into a channel also works
      p1.c <~ [[target_addr, 50, 100]]
    }
    sleep 1
    p2.sync_do {
      assert_equal([[target_addr, 10, 20], [target_addr, 50, 100]], p2.recv.to_a.sort)
    }

    # Check that inserting into a channel via <= is rejected
    assert_raise(Bud::BudError) {
      p1.sync_do {
        p1.c <= [[target_addr, 60, 110]]
      }
    }

    # Check that key constraints on channels are raised
    assert_raise(Bud::KeyConstraintError) {
      p1.sync_do {
        p1.c <~ [[target_addr, 70, 120]]
        p1.c <~ [[target_addr, 70, 130]]
      }
    }

    p1.stop_bg
    p2.stop_bg
  end
end
