require "rubygems"
require "bud"

RING_SIZE = 20
NUM_CIRCUITS = 800
NUM_MESSAGES = RING_SIZE * NUM_CIRCUITS

class RingMember
  include Bud

  state do
    channel :pipe, [:@addr, :cnt, :val]
    scratch :kickoff, [:cnt, :val]
    table :next_guy, [:addr]
    table :last_cnt, [:cnt]
    scratch :done, [:cnt]
  end

  bloom :ring_msg do
    pipe <~ kickoff {|k| [ip_port, k.cnt, k.val]}
    pipe <~ (pipe * next_guy).pairs {|p,n| [n.addr, p.cnt + 1, p.val] if p.cnt < NUM_MESSAGES}
    done <= pipe {|p| [p.cnt] if p.cnt == NUM_MESSAGES}
  end

  # We do some minor computation as well as just routing the message onward
  bloom :update_log do
    last_cnt <+ pipe {|p| [p.cnt]}
    last_cnt <- (pipe * last_cnt).pairs {|p, lc| [lc.cnt]}
  end
end

def test_basic_ring
  ring = []
  RING_SIZE.times do |i|
    ring[i] = RingMember.new
    ring[i].run_bg
  end
  q = Queue.new
  ring.last.register_callback(:done) do
    q.push(true)
  end

  ring.each_with_index do |r, i|
    next_idx = i + 1
    next_idx = 0 if next_idx == RING_SIZE
    next_addr = ring[next_idx].ip_port

    r.sync_do {
      r.next_guy <+ [[next_addr]]
    }
  end

  first = ring.first
  first.async_do {
    first.kickoff <+ [[1, "xyz"]]
  }

  # Wait for the "done" callback from the last member of the ring.
  q.pop

  ring.each_with_index do |r, i|
    # XXX: we need to do a final tick here to ensure that each Bud instance
    # applies pending <+ and <- derivations. See issue #50.
    r.sync_do
    r.stop
    expected = (NUM_MESSAGES - RING_SIZE) + i + 1
    raise unless r.last_cnt.first == [expected]
  end
end

test_basic_ring
