require 'rubygems'
require 'bud'

require 'lib/voting'
require 'lib/nonce'

class LeaderElection < Bud
  include MajorityVotingMaster
  include VotingAgent

  def initialize(i, p, id)
    super i, p, {'dump' => true}
    @nonce = Nonce.new(i, p.to_i + 1, {'dump' => true})
    @nonce.tick
    @id = id
  end
  def state
    super
    #blackboard :current_state, [], ['status', 'leader', 'vid']
    table :current_state, [], ['status', 'leader', 'vid']
    scratch :will_ballot, ['nonce', 'vid', 'time']
    ##table :ballot_history, ['nonce', 'vid', 'time']
    scratch :latest_ballot, ['time']

    periodic :timer, 1
    ##table :seen_ballots, ['peer', 'candidate', 'nonce']
    scratch :will_vote, ['message', 'leader', 'vid']
    scratch :found_leader, ['ballot', 'leader', 'vid']
  end

  def tick
    @nonce.tick
    super
  end

  declare
  def decide
      will_vote <= join([ballot, current_state]).map do |b, c|
        if c.status == "election" and b.content.fetch(2) >= c.vid
          print "will vote\n" or [b.content, b.content.fetch(1), b.content.fetch(2)]
        else
          print "no vote?" + b.inspect + "," + c.inspect + "\n" or [b.content, c.leader, c.vid]
        end
      end
    
      cast_vote <+ will_vote.map{|w| [w.message, [w.leader, w.vid]]}
      current_state <+ will_vote.map{|w| ['election', w.leader, w.vid]}
      current_state <- join([will_vote, current_state]).map{|w, c| c}
  end

  declare
  def le_two 
      nj = join [timer, current_state, @nonce.nonce]
      #@nonce.nonce <- nj.map do |t, s, n| 
      #  if s.status == 'election'
      #    print "(#{budtime} delete nonce #{n.inspect}\n" or n
      #  end
      #end

      will_ballot <= nj.map do |t, s, n|
        if s.status == "election" 
          print "(#{budtime}) will ballot #{n.id}\n" or [n.id, s.vid, Time.new.to_i]
        end
      end

      begin_vote <+ will_ballot.map{|w|  [w.nonce, [w.nonce, @myloc, w.vid]] }

      found_leader <+ join([current_state, vote_status]).map do |c, s|
        #print "found leader? #{v.cnt} for #{v.vote[0]}\n"
        if c.status == "election" 
          print "found leader?\n" or [s.id, s.content.fetch(1), s.response]
        end
      end

      current_state <+ found_leader.map do |c, s|
        if f.leader == @myloc
          print "setting to leader\n" or ['leader', @myloc, f.vid]
        else
          print "setting to follower\n" or ['follower', f.leader, f.vid]
        end
      end

      current_state <- found_leader.map{|f, c| c}
      #status <= found_leader.map{|f| [f.ballot, f.leader] }
  end
end


