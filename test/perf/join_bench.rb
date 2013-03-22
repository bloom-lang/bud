require "rubygems"
require "bud"
require "benchmark"

JOIN_INPUT_SIZE = 20000
NUM_RUNS = 10

class HashJoinBench
  include Bud

  state do
    table :t1
    table :t2
    table :t3
  end

  bootstrap do
    # Only a single pair of tuples satisfy the join condition
    JOIN_INPUT_SIZE.times do |i|
      t1 << [i, i + 50000]
      t2 << [i + JOIN_INPUT_SIZE - 1, i]
    end
  end

  bloom do
    t3 <= (t1 * t2).pairs(:key => :key)
    stdio <~ t3 {|t1, t2| ["Join result: #{[t1,t2].inspect}"]}
  end
end

b = HashJoinBench.new
b.tick
t = Benchmark.measure do
  NUM_RUNS.times do
    b.t1 <- [[0, 50000]]
    b.t1 <+ [[0, 50000]]
    b.tick
  end
end
puts "Time taken for #{NUM_RUNS} joins: #{t}"
