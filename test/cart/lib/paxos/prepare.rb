require 'rubygems'
require 'bud'

require 'lib/voting'

class PaxosPrepare < MajorityVotingMaster
  def state
    super
    table :local_aru, [], ['host', 'aru']
    scratch :leader_change, ['host'], ['leader', 'view']
  
    scratch :prepare, ['view', 'aru']
    table :quorum, ['view', 'aru']
  end

  declare 
  def prep1
    #local_aru << [@myloc, 0] if global_history.empty?
    #last_installed << [0] if global_history.empty?

    prepare <= join([leader_change, local_aru]).map do |c, a|
      #print "um\n"
      if c.leader == c.host
        print "prepare!\n" or [c.view, a.aru]
      end
    end

    begin_vote <+ prepare.map{|p| print "put in ballot : " + p.inspect + "\n" or [p.view, p]}
  end

  declare 
  def establish_quorum
    quorum <= vote_status.map do |v|
      if v.response.class == Array 
        [ v.response.fetch(1), v.response.fetch(2) ] if v.response.fetch(4) == 'bottom'
      end
    end
  end
end

class PaxosPrepareAgent < VotingAgent
  def state
    super
    table :datalist, ['message', 'view', 'aru_requested', 'seq', 'update', 'type']
    table :datalist_length, ['aru', 'len']
    table :global_history, ['host', 'seqno'], ['requestor', 'update']
    table :last_installed, [], ['view']
    table :accept, ['view', 'seq', 'update']
  end 

  declare
  def build_reply
    datalist <= join([ballot, last_installed]).map do |d, l|
      if d.content.fetch(1) == l.view
        print "AROO\n" or [d.content, d.content.fetch(0), d.content.fetch(1), -1, "none", "bottom"]
      else 
        print "ACHOO " + d.inspect + ":: " + l.inspect + " vs. " +d.content.fetch(0).to_s + "\n"
      end
    end

    datalist <= join([datalist, global_history]).map do |d, g|
      if g.seqno > d.aru_requested and d.type == "bottom"
        print "oh yeah\n" or [d.content, d.view, d.aru_requested, g.seqno, g.update, "ordered"]
      else
        print "oh dear.  !" + g.seqno.to_s + " > " + d.aru_requested.to_s + "\n"
      end 
    end

    datalist <= join([datalist, accept]).map do |d, a|
      if a.seq >= d.aru and d.type == "bottom"
        [d.message, d.view, d.aru_requested, a.seq, a.update, "proposed"]
      else
        print "oh dear. !" + a.seq.to_s + " >= " + d.aru.to_s + "\n"
      end
    end

    datalist_length <= datalist.group([datalist.aru_requested], count())
  end

  declare
  def caster
    dj = join([datalist, datalist_length])
    cast_vote <+ dj.map do |d, l|
      #print "SEDNING vote!\n"
      print "SEND " +d.view.to_s + ": " + d.inspect + "\n" or [d.view, [d.view, d.aru_requested, d.seq, d.update, d.type, l.len]]
    end
  
    datalist <- dj.map{|d, l| d}
  end 
end

