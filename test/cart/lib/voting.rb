require 'rubygems'
require 'bud'

class VoteInterface < Bud
  def initialize(i, p, o = nil)
    super(i, p, o)
    @addy = "#{ip}:#{port}"
  end

  # if we aren't spmd, we need to define both ends of the channel.
  def state
    channel :ballot, 0, ['peer', 'master', 'id'], ['content']
    channel :vote, 0, ['master', 'peer', 'id'], ['response']
    channel :tickler, 0, ['master']
  end
end

class VotingMaster < VoteInterface
  def state
    super
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
    ballot <+ join([begin_vote, member]).map {|b, m| [m.peer, @addy, b.id, b.content] }
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


class VotingAgent < VoteInterface
  def state
    super
    table :peer_ballot_cache, ['id', 'content', 'master']
    scratch :cast_vote, ['id', 'response']
  end
  
  declare 
  def casting
    peer_ballot_cache <= ballot.map{|b| [b.id, b.content, b.master] }
    # if there is a standing ballot, send a vote if stimulated by cast_vote
    # the voters insert into cast_vote after considering peer_ballot_cache.
    vote <+ join([cast_vote, peer_ballot_cache], [cast_vote.id, peer_ballot_cache.id]).map do |v, c| 
      [c.master, @addy, v.id, v.response] 
    end
  end
end
