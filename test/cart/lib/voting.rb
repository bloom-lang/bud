require 'rubygems'
require 'bud'

module VoteInterface
  include BudState
  # channels used by both ends of the voting protocol
  def self.extended(base)
    channel :ballot, ['@peer', 'master', 'id'], ['content']
    channel :vote, ['@master', 'peer', 'id'], ['response']
    channel :tickler, ['@master']
  end
end

class VotingMaster < Bud
  include VoteInterface
  
  def state
    extend VoteInterface
    # local interfaces    
    scratch :begin_vote, ['id', 'content']
    scratch :victor, ['id', 'content', 'response']

    table :vote_status, ['id', 'content', 'response']
    table :member, ['peer']
    table :master_vote_cache, ['id', 'response', 'peer']
    table :member_cnt, ['cnt']
    table :vote_cnt, ['id', 'response', 'cnt']
  end

  declare
  def initiation
    # multicast ballots when stimulated by begin_vote
    ballot <~ join([begin_vote, member]).map {|b, m| [m.peer, @ip_port, b.id, b.content] }
    vote_status <+ begin_vote.map{|b| [b.id, b.content, 'in flight'] }
    member_cnt <= member.group(nil, count)
  end

  declare
  def counting
    master_vote_cache <= vote.map{|v| [v.id, v.response, v.peer] }
    vote_cnt <= master_vote_cache.group([master_vote_cache.id, master_vote_cache.response], count(master_vote_cache.peer))
  end

  declare
  def summary
    # a subclass will likely override this particular summary of votes.
    # here, we enforce a unanimous vote and choose the "response", among
    # possibly many, that has as many votes as there are members.
    victor <= join([vote_status, member_cnt, vote_cnt], [vote_status.id, vote_cnt.id]).map do |s, m, v|
      if m.cnt == v.cnt
        [v.id, s.content, v.response]
      end
    end 
    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map{|v| [v.id, v.content, 'in flight'] }
  end

end


class VotingAgent < Bud
  def state
    extend VoteInterface
    table :waiting_ballots, ['id', 'content', 'master']
    scratch :cast_vote, ['id', 'response']
  end

  # default for decide: always cast vote 'yes'.  expect subclasses to override.
  declare 
  def decide
    cast_vote <= waiting_ballots.map{ |b| [b.id, 'yes'] }
  end
  
  declare 
  def casting
    # cache incoming ballots for subsequent decisions (may be delayed)
    waiting_ballots <= ballot.map{|b| [b.id, b.content, b.master] }
    # whenever we cast a vote on a waiting ballot, send the vote
    vote <~ join([cast_vote, waiting_ballots], [cast_vote.id, waiting_ballots.id]).map do |v, c| 
      [c.master, @ip_port, v.id, v.response] 
    end
  end
end
