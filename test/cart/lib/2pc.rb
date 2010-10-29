require 'lib/voting'

class TwoPCAgent < VotingAgent
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  def state
    super
  end

end


class TwoPCMaster < VotingMaster
  # 2pc is a specialization of voting:
  # * ballots describe transactions
  # * voting is Y/N.  A single N vote should cause abort.
  def state
    super
    table :xact, ['xid', 'data'], ['status']
    scratch :request_commit, ['xid'], ['data']
  end
  
  declare 
  def boots
    xact <= request_commit.map{|r| [r.xid, r.data, 'prepare'] }
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

    xact <+ decide.map do |x, s|
      [x.xid, x.data, "commit"] if s.response == "Y"
    end
  end
  
end
