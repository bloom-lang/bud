require 'rubygems'
require 'bud'

class PermissiveVoter < Vote
  def declaration
    strata[2] = rules {
      vcnt <= join([ballot, mcnt]).map do |b, m|
        [ballot.ballot, 
      end
    }
  end
end


require 'examples/vote'

class PaxosPrepare < PermissiveVoter
  def state
    super
    table :local_aru, [], ['host', 'aru']
    table :global_history, ['host', 'seqno'], ['requestor', 'update']
    scratch :leader_change, ['host'], ['leader', 'view']
  
    scratch :prepare, ['view', 'aru']
    table :last_installed, [], ['view']
    table :accept, ['view', 'seq', 'update']

    scratch :datalist, ['message', 'view', 'aru_requested', 'seq', 'update', 'type']
    scratch :datalist_length, ['aru', 'len']

  end

  def declaration
    super
    strata[2] = rules {
      local_aru << [@myloc, 0] if global_history.empty?
      last_installed << [0] if global_history.empty?

      prepare <= join([leader_change, local_aru]).map do |c, a|
        print "um\n"
        if c.leader == c.host
          print "prepare!\n"
          [c.view, a.aru]
        end
      end

      ballot <+ prepare.map{|p| print "put in ballot : #{p.inspect}\n"; [p]}

      deliver.each {|d| print "Deliver: #{d.inspect}\n"}

      datalist <= join([deliver, last_installed]).map do |d, l|
        print "datalist loop\n"
        view = d.message[0] 
        aru = d.message[1]
        [d.message, view, aru, -1, "none", "bottom"] if view = l.view
      end

      datalist <= join([datalist, global_history]).map do |d, g|
        if g.seqno > d.aru_requested and d.type == "bottom"
          [d.message, d.view, d.aru_requested, g.seqno, g.update, "ordered"]
        end 
      end

      datalist <= join([datalist, accept]).map do |d, a|
        if a.seq >= d.aru and d.type == "bottom"
          [d.message, d.view, d.aru_requested, a.seq, a.update, "proposed"]
        end
      end

      datalist_length <= datalist.group([datalist.aru_requested], count())

    }
    strata[3] = rules {
      vote <+ join([datalist, datalist_length]).map do |d, l|
        print "SEDNING vote!\n"
        [d.message, [d.view, d.aru_requested, d.seq, d.update, d.type, l.len]]
      end
    }
    
  end 
end


p = PaxosPrepare.new('127.0.0.1', 10001)
p.tick
p.member << ['127.0.0.1', 10001]

p.leader_change <+ [['127.0.0.1:10001', '127.0.0.1:10001', 5]]


p.tick

sleep 1

p.tick
sleep 1

p.tick
sleep 1


sleep 4

