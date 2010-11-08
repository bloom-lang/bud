require 'lib/voting'

module TwoPCAgent
  # boilerplate!!
  include Anise
  annotator :declare 
  include VotingAgent
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  def state
    super if defined? super
    scratch :can_commit, ['xact', 'decision']
  end

  declare
  def decide
    cast_vote <= join([waiting_ballots, can_commit], [waiting_ballots.id, can_commit.xact]).map{|w, c| [w.id, c.decision] }
  end

end


module TwoPCVotingMaster
  # boilerplate
  include Anise
  annotator :declare 
  include VotingMaster
  # override the default summary s.t. a single N vote
  # makes the vote_status = ABORT
  def summary
    victor <= join([vote_status, member_cnt, vote_cnt], [vote_status.id, vote_cnt.id]).map do |s, m, v|
      if v.response == "N"
        [v.id, s.content, "N"]
      elsif v.cnt > m.cnt / 2
        [v.id, s.content, v.response]
      end
    end

    vote_status <+ victor.map{|v| v }
    vote_status <- victor.map{|v| [v.id, v.content, 'in flight'] }
    localtick <~ victor.map{|v| v}
  end
end


module TwoPCMaster
  # boilerplate
  include Anise
  annotator :declare
  include TwoPCVotingMaster
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  def state
    super if defined? super
    
    table :xact, ['xid', 'data'], ['status']
    scratch :request_commit, ['xid'], ['data']
  end
  
  declare 
  def boots
    xact <= request_commit.map{|r| [r.xid, r.data, 'prepare'] }
    stdio <~ request_commit.map{|r| ["begin that vote"]}
    begin_vote <= request_commit.map{|r| [r.xid, r.data] }
  end

  declare
  def panic_or_rejoice
    decide = join([xact, vote_status], [xact.xid, vote_status.id])
    xact <+ decide.map do |x, s|
      [x.xid, x.data, "abort"] if s.response == "N"
    end

    xact <- decide.map do |x, s|
      x if s.response == "N"
    end

    stdio <~ decide.map { |x, s| ["COMMITTING"] if s.response == "Y" }
    xact <+ decide.map { |x, s| [x.xid, x.data, "commit"] if s.response == "Y" }
  end
  
end

module Monotonic2PCMaster
  # boilerplate
  include Anise
  annotator :declare
  include VotingMaster
  def initialize(i, p, o)
    super(i, p, o)
    xact_order << ['prepare', 0]
    xact_order << ['commit', 1]
    xact_order << ['abort', 2]
  end
  def state
    super if defined? super
    
    # TODO
    table :xact_order, ['status'], ['ordinal']
    table :xact_final, ['xid', 'ordinal']
    scratch :xact, ['xid', 'data', 'status']
    table :xact_accum, ['xid', 'data', 'status']
    scratch :request_commit, ['xid'], ['data']
    scratch :sj, ['xid', 'data', 'status', 'ordinal']
  end
  
  declare 
  def boots
    xact_accum <= request_commit.map{|r| [r.xid, r.data, 'prepare'] }
    begin_vote <= request_commit.map{|r| [r.xid, r.data] }
  end

  declare
  def panic_or_rejoice
    decide = join([xact_accum, vote_status], [xact_accum.xid, vote_status.id])
    xact_accum <= decide.map do |x, s|
      [x.xid, x.data, "abort"] if s.response == "N"
    end

    xact_accum <= decide.map do |x, s|
      [x.xid, x.data, "commit"] if s.response == "Y"
    end
  end
  
  declare
  def twopc_status
    sj <= join([xact_accum, xact_order], [xact_accum.status, xact_order.status]).map do |x,o|
      [x.xid, x.data, x.status, o.ordinal]
    end
    xact_final <= sj.group([sj.xid], max(sj.ordinal))
    xact <= join( [sj, xact_final], [sj.ordinal, xact_final.ordinal]).map do |s, x|
      [s.xid, s.data, s.status]
    end
  end
end
