require 'rubygems'
require 'bud'

require 'cidr/reliable_delivery'

rd = ReliableDelivery.new('localhost', 12345)
rd2 = ReliableDelivery.new('localhost', 12346)
rd.run_bg
rd2.run_bg

sleep 1

(0..ARGV[0].to_i).each do |i|
  rd.pipe <+ [["localhost:12346", "localhost:12345", i, 'foobar'+i.to_s]]
end

cnt = 0
while cnt < 51
  cnt = rd.pipe_out.length
  print "CNT #{cnt}\n"
  sleep 3
end

print "DONE!\n"
