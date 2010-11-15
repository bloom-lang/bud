require 'rubygems'
require 'bud'

module VoteInterface
  # channels used by both ends of the voting protocol
  # paa: TODO: figure out the right way to mix in state
  def state
    super if defined? super
    channel :ballot, ['@peer', 'master', 'ident'], ['content']
    channel :vote, ['@master', 'peer', 'ident'], ['response']
    channel :tickler, ['@master'] unless defined? tickler
  end
end

module VotingMaster
  # boilerplate
  include Anise
  include VoteInterface
  annotator :declare

  def state
    super if defined? super
    # local interfaces    
    interface input, :begin_vote, ['ident', 'content']
    interface output, :victor, ['ident', 'content', 'response']

    table :vote_status, 
          ['ident', 'content', 'response']
    table :member, ['peer']
    table :votes_rcvd, ['ident', 'response', 'peer']
    scratch :member_cnt, ['cnt']
    scratch :vote_cnt, ['ident', 'response', 'cnt']
  end

  declare
  def initiation
    # when stimulated by begin_vote, send ballots 
    # to members, set status to 'in flight'
    j = join([begin_vote, member])
    ballot <~ j.map do |b,m| 
      [m.peer, @ip_port, b.ident, b.content] 
    end
    vote_status <+ begin_vote.map do |b| 
      [b.ident, b.content, 'in flight'] 
    end
    member_cnt <= member.group(nil, count)
  end

  declare
  def counting
    # accumulate votes into votes_rcvd table, 
    # calculate current counts
    stdio <~ vote.map { |v| ["GOT VOTE: " + v.inspect] }
    votes_rcvd <= vote.map { |v| [v.ident, v.response, v.peer] }
    vote_cnt <= votes_rcvd.group(
      [votes_rcvd.ident, votes_rcvd.response], 
      count(votes_rcvd.peer))
  end

  declare
  def summary
    # this stub changes vote_status only on a 
    # complete and unanimous vote.
    # a subclass will likely overridente this
    # paa -- fix potentially global scope of join aliases somehow...
    sj = join([vote_status, member_cnt, vote_cnt], 
             [vote_status.ident, vote_cnt.ident])
    victor <= sj.map do |s,m,v|
      if m.cnt == v.cnt
        [v.ident, s.content, v.response]
      end
    end
    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map do |v| 
      [v.ident, v.content, 'in flight'] 
    end
    localtick <~ victor.map{|v| v}
  end
end


module VotingAgent
  include Anise
  annotator :declare
  include VoteInterface

  def state
    super if defined? super
    table :waiting_ballots, ['ident', 'content', 'master']
    interface input, :cast_vote, ['ident', 'response']
  end

  # default for decidente: always cast vote 'yes'.  expect subclasses to overridente.
  declare 
  def decidente
    cast_vote <= waiting_ballots.map{ |b| print "EMPTY cast\n" or [b.ident, 'yes'] }
  end
  
  declare 
  def casting
    # cache incoming ballots for subsequent decisions (may be delayed)
    waiting_ballots <= ballot.map{|b| [b.ident, b.content, b.master] }
    stdio <~ ballot.map{|b| ["PUT"] }
    # whenever we cast a vote on a waiting ballot, send the vote
    vote <~ join([cast_vote, waiting_ballots], [cast_vote.ident, waiting_ballots.ident]).map do |v, c| 
      [c.master, @ip_port, v.ident, v.response] 
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
    victor <= join([vote_status, member_cnt, vote_cnt], [vote_status.ident, vote_cnt.ident]).map do |s, m, v|
      if v.cnt > m.cnt / 2
        [v.ident, s.content, v.response]
      end
    end 
    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map{|v| [v.ident, v.content, 'in flight'] }
    localtick <~ victor.map{|v| v}
  end

end
