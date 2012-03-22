require "rubygems"
require "bud"

BENCH_LIMIT = 30000

class ScratchBench
  include Bud

  state do
    scratch :t1, [:key]
    scratch :done
  end

  bloom do
    t1 <= t1 {|t| [t.key + 1] if t.key < BENCH_LIMIT}
    done <= t1 {|t| t if t.key >= BENCH_LIMIT}
  end
end

b = ScratchBench.new
b.run_bg
b.sync_do {
  b.t1 <+ [[0]]
}
b.stop
