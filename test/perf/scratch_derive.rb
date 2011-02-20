require "rubygems"
require "bud"

class ScratchBench
  include Bud

  BENCH_LIMIT = 200

  state do
    scratch :t1, [:key]
    scratch :done
  end

  declare
  def bench
    t1 <= t1.map {|t| [t.key + 1] if t.key < ScratchBench::BENCH_LIMIT}
    done <= t1.map {|t| t if t.key >= ScratchBench::BENCH_LIMIT}
  end
end

b = ScratchBench.new
b.run_bg
b.sync_do {
  b.t1 <+ [[0]]
}
b.stop_bg
