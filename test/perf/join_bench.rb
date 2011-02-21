require "rubygems"
require "bud"
require "benchmark"

JOIN_INPUT_SIZE = 10000

class HashJoinBench
  include Bud

  state do
    table :t1
    table :t2
  end

  bootstrap do
    # Only a single pair of tuples satisfy the join condition
    JOIN_INPUT_SIZE.times do |i|
      t1 << [i, i + 50000]
      t2 << [i + JOIN_INPUT_SIZE - 1, i]
    end
  end

  declare
  def do_join
    j = join([t1, t2], [t1.key, t2.key])
    stdio <~ j.map {|t1, t2| ["Join result: #{[t1,t2].inspect}"]}
  end
end

b = HashJoinBench.new
b.run_bg
t = Benchmark.measure do
  b.sync_do {
    9.times do
      b.tick
    end
  }
end
puts "Time taken for 10 joins: #{t}"
b.stop_bg
