require 'rubygems'
require 'bud'

require 'examples/simple_broadcast'

class Vote < SimpleBroadcast
  #def initialize(ip, port)
  #  super ip, port
  #end

  def state
    # ideally this would 'just happen' as part of inheritance...
    super
    scratch :ballot, ['ballot']
    table :master, ['master']
    channel :vote, 0, ['otherloc', 'myloc', 'ballot'], ['vote']  
    table :vote_ledger, ['otherloc', 'myloc', 'ballot'], ['vote']  
    scratch :mcnt, ['master'], ['cnt']
    scratch :vcnt, ['ballot', 'vote'], ['cnt']
    scratch :status, ['ballot', 'status']    
    periodic :timer, 1    
  end

  def declaration
    super
    # jacked up: pick a stratum that won't clobber those instantiated by our superclass
    strata[1] = rules {
      mcnt <= members.group([members.myloc], count())
      vcnt <= vote_ledger.group([vote_ledger.ballot, vote_ledger.vote], count())
      message <+ ballot.map{|b| print "add #{b}\n"; b}

      vote_ledger <= vote.map{|v| print "got a vote: #{v.inspect}\n"; v}

      # some code that inserts into vote
    }

  end
end

class VoteUser < Vote
  def state
    super
  end
  def declaration 
    super
    strata[2] = rules {
      j = join [deliver, master]
      vote <+ j.map{|d, m| print "vote for #{d.message}.   send to #{m.master}\n"; [m.master, @myloc, d.message, 'Y'] }

      j2 = join [mcnt, vcnt]
      status <+ j2.map do |m, c|
        print "#{m.cnt} vs. #{c.inspect}\n"

          vote_ledger.each {|v| puts v.inspect }
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
