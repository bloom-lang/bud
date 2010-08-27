require 'rubygems'
require 'bud'

require 'examples/vote'

class VoteUser < Vote
  def state
    super
  end
  def declaration 
    super
    strata[2] = rules {
      j = join [deliver, master]
      vote <+ j.map{|d, m| print "#{@myloc} vote for #{d.message}.   send to #{m.master}\n"; [m.master, @myloc, d.message, 'Y'] }

      j2 = join [mcnt, vcnt]
      status <+ j2.map do |m, c|
        print "#{m.cnt} vs. #{c.inspect}\n"

          #vote_ledger.each {|v| puts v.inspect }
          #vcnt.each {|v| puts v.inspect }
        if m.cnt == c.cnt and c.vote == "Y"
          print "OK! #{c.ballot}\n"
          [c.ballot, "Success"]
        end
      end
    }
  end
end


v = VoteUser.new("127.0.0.1", ARGV[0])
v.tick
v.master << ['127.0.0.1:10001']

if ARGV[0] == '10001'
  v.run_bg
  sleep 1
  print "M\n"
  v.member << ['127.0.0.1:10001']
  v.member << ['127.0.0.1:10002']
#  v.member << ['127.0.0.1:10003']
  
  v.ballot <+ [['foobar']]

  #v.run

  (0..50).each do |i|
    print "I #{i}\n"
    v.tickle
    sleep 1
    #v.ballot <+ [['bambam']] if i == 3 and ARGV[0] == '10001'
    v.status.each {|s| print "S: #{s.inspect}\n"; } 
  end

else
  print "N\n"
  v.member << ['127.0.0.1:10001']
  v.run_bg
  (0..50).each do |i|
    v.tickle
    sleep 1
  end
end
