require 'rubygems'
require 'bud'
require 'benchmark'

require 'cidr/reliable_delivery'

here = 'ip-10-203-91-184.ec2.internal:12345'
there = 'ip-10-202-70-4.ec2.internal:12346'

rd = ReliableDelivery.new('ip-10-203-91-184.ec2.internal', 12345)
#rd2 = ReliableDelivery.new('localhost', 12346)
rd.run_bg
#rd2.run_bg

sleep 1

puts Benchmark.measure { 

(0..ARGV[0].to_i).each do |i|
  rd.pipe <+ [[there, here, i, 'foobar'+i.to_s]]
end

cnt = 0
while cnt < ARGV[0].to_i + 1
  cnt = rd.pipe_out.length
  print "CNT #{cnt}\n"
  sleep 3
end

print "DONE!\n"
}
