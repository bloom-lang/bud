require "rubygems"
require "bud"
require "fileutils"

BENCH_LIMIT = 200

class TcBench
  include Bud

  state do
    tctable :t1, [:key]
    scratch :done
  end

  declare
  def bench
    t1 <= t1.map {|t| [t.key + 1] if t.key < BENCH_LIMIT}
    done <= t1.map {|t| t if t.key >= BENCH_LIMIT}
  end
end

dir = File.dirname(__FILE__) + "/tc_tmp"
b = TcBench.new(:tc_dir => dir, :truncate => true)
b.run_bg
b.sync_do {
  b.t1 <+ [[0]]
}
b.stop_bg
FileUtils.rm_r(dir)
