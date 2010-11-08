require 'rubygems'
require 'bud'

module VoteInterface
  # channels used by both ends of the voting protocol
  # paa: TODO: figure out the right way to mix in state
  def self.extended(base)
    base.channel :ballot, ['@peer', 'master', 'id'], ['content']
    base.channel :vote, ['@master', 'peer', 'id'], ['response']
    base.channel :tickler, ['@master']
  end
end

module VotingMaster
  # boilerplate
  include Anise
  annotator :declare

  def state_vm
    extend VoteInterface
    # local interfaces    
    scratch :begin_vote, ['id', 'content']
    scratch :victor, ['id', 'content', 'response']

    table :vote_status, 
          ['id', 'content', 'response']
    table :member, ['peer']
    table :votes_rcvd, ['id', 'response', 'peer']
    scratch :member_cnt, ['cnt']
    scratch :vote_cnt, ['id', 'response', 'cnt']
  end

  declare
  def initiation
    # when stimulated by begin_vote, send ballots 
    # to members, set status to 'in flight'
    j = join([begin_vote, member])
    ballot <~ j.map do |b,m| 
      [m.peer, @ip_port, b.id, b.content] 
    end
    vote_status <+ begin_vote.map do |b| 
      [b.id, b.content, 'in flight'] 
    end
    member_cnt <= member.group(nil, count)
  end

  declare
  def counting
    # accumulate votes into votes_rcvd table, 
    # calculate current counts
    votes_rcvd <= vote.map do |v| 
      print "GOT VOTE: " + v.inspect + "\n" or [v.id, v.response, v.peer] 
    end
    vote_cnt <= votes_rcvd.group(
      [votes_rcvd.id, votes_rcvd.response], 
      count(votes_rcvd.peer))
  end

  declare
  def summary
    # this stub changes vote_status only on a 
    # complete and unanimous vote.
    # a subclass will likely override this
    # paa -- fix potentially global scope of join aliases somehow...
    sj = join([vote_status, member_cnt, vote_cnt], 
             [vote_status.id, vote_cnt.id])
    victor <= sj.map do |s,m,v|
      if m.cnt == v.cnt
        [v.id, s.content, v.response]
      end
    end
    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map do |v| 
      [v.id, v.content, 'in flight'] 
    end
  end
end


module VotingAgent
  include Anise
  annotator :declare
  def state_va
    extend VoteInterface
    table :waiting_ballots, ['id', 'content', 'master']
    scratch :cast_vote, ['id', 'response']
  end

  # default for decide: always cast vote 'yes'.  expect subclasses to override.
  declare 
  def decide
    cast_vote <= waiting_ballots.map{ |b| print "EMPTY cast\n" or [b.id, 'yes'] }
  end
  
  declare 
  def casting
    # cache incoming ballots for subsequent decisions (may be delayed)
    waiting_ballots <= ballot.map{|b| print "PUT\n" or [b.id, b.content, b.master] }
    # whenever we cast a vote on a waiting ballot, send the vote
    vote <~ join([cast_vote, waiting_ballots], [cast_vote.id, waiting_ballots.id]).map do |v, c| 
      [c.master, @ip_port, v.id, v.response] 
    end
  end
end


module MajorityVotingMaster 
  # boilerplate
  include Anise
  annotator :declare
  include VotingMaster
  declare
  def summary
    victor <= join([vote_status, member_cnt, vote_cnt], [vote_status.id, vote_cnt.id]).map do |s, m, v|
      if v.cnt > m.cnt / 2
        [v.id, s.content, v.response]
      end
    end 
    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map{|v| [v.id, v.content, 'in flight'] }
  end

end
