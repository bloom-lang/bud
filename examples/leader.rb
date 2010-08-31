require 'rubygems'
require 'bud'

require 'examples/vote'
require 'examples/nonce'

class LeaderElection < Vote

  def initialize(i, p, id)
    super i, p
    @nonce = Nonce.new(i, p.to_i + 1)
    @nonce.tick
    @id = id
  end
  def state
    super
    table :current_state, [], ['status', 'leader', 'vid'], "last"
    scratch :will_ballot, ['nonce', 'vid', 'time']
    table :ballot_history, ['nonce', 'vid', 'time']
    scratch :latest_ballot, ['time']

    periodic :timer, 1
    table :seen_ballots, ['peer', 'candidate', 'nonce']
    scratch :will_vote, ['master', 'message', 'leader', 'vid']
    scratch :found_leader, ['ballot', 'leader', 'vid']
  end

  def tick
    @nonce.tick
    super
  end

  def declaration 
    super
    strata[2] = rules {
      # maybe I am the initiator...
      current_state << ['election', @myloc, @id] if current_state.empty?
      will_vote <= join([deliver, current_state]).map do |d, c|
        #nonce = d.message[0]
        ldr = d.message[1]       
        nvid = d.message[2]
        #print "got to reply to #{d.otherloc}\n"
        if c.status == "election" && nvid > c.vid
          [d.otherloc, d.message, ldr, nvid]
        else
          [d.otherloc, d.message, c.leader, c.vid]
        end
      end
    
      #vote <+ will_vote.map{|w| [w.master, @myloc, w.message, [w.leader, w.vid]]}
      vote <+ will_vote.map{|w| [w.message, [w.leader, w.vid]]}
      current_state <+ will_vote.map{|w| ['election', w.leader, w.vid]}
      current_state <- join([will_vote, current_state]).map{|w, c| c}
    }

    strata[3] = rules {
      j = join [current_state, @nonce.nonce, timer]
      @nonce.nonce <- j.map do |s, n, t| 
        if s.status == 'election'
          print "(#{budtime} delete nonce #{n.inspect}\n";  
          n
        end
      end

      will_ballot <= j.map do |s, n, t|
        if s.status == "election" 
          print "(#{budtime}) will ballot #{n.id}\n"
          [n.id, s.vid, Time.new.to_i]
        end
      end

      #latest_ballot <= will_ballot.group(nil, max(time)) 
      ballot <+ will_ballot.map{|w| print "ballot up #{w.inspect}\n"; [[w.nonce, @myloc, w.vid]] }

      found_leader <+ join([current_state, mcnt, vcnt]).map do |c, m, v|
        #print "found leader? #{v.cnt} for #{v.vote[0]}\n"
        if v.cnt > (m.cnt / 2) and c.status == "election"
          #print "in there: #{m.cnt} > half #{c.cnt}\n"
          [v.ballot, v.vote[0], v.vote[1]]
        end
      end

      j3 = join [found_leader, current_state]
      current_state <+ j3.map do |f, c|
        if f.leader == @myloc
          print "setting to leader\n"
          ['leader', @myloc, f.vid]
        else
          print "setting to follower\n" 
          ['follower', f.leader, f.vid]
        end
      end

      current_state <- j3.map{|f, c| c}
      status <= found_leader.map{|f| [f.ballot, f.leader] }
    }
  end
end


