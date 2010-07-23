# two-phase commit.
# incomplete

require 'rubygems'
require 'bud'

class TwoPC < Bud
  def initialize(ip, port)
    super ip, port
  end

  def state
    scratch :peer_cnt, ['coord'], ['cnt']
    table   :peers,    ['coord', 'peer']
    scratch :yes_cnt,  ['coord', 'xid'], ['cnt']
    table   :vote,     ['coord', 'xid', 'peer'], ['vote']
    table   :xact,     ['node', 'xid'], ['state']
    channel :results,  ['node', 'xid'], ['state']
  end
  
  def declaration
    strata[0] = rules {
      # Count number of peers 
      peers.reduce(peer_cnt) do |memo,p| 
        memo[[p.coord]] = memo[[p.coord]].nil? ? [1] : [memo[[p.coord]][0] + 1] 
      end
      # Count number of "yes" votes 
      vote.reduce(yes_cnt) do |memo,v|
        if v.vote == "yes" then
          memo[[v.coord, v.xid]] = memo[[v.coord, v.xid]].nil? ? [1] : [memo[[v.coord, v.xid]][0] + 1] 
        end
      end
    }
    
    strata[1] = rules {
      # Prepare => Commit if unanimous 
      j = join(peer_cnt, yes_cnt, xact)
      j.map do |p, y, x| 
        if p.coord == y.coord and y.coord == x.coord and y.xid == x.xid and p.cnt == y.cnt and x.state == 'prepare' then
          xact <= [p.coord, y.xid, "commit"] 
        end
      end
      # Prepare => Abort if any "no" votes 
      k = join(vote, xact)
      k.map do |v, x|
        if v.coord == x.node and v.xid == x.xid and v.vote == "no" and x.state == "prepare" then
          xact <= [k.coord, x.xid, "abort"]  
        end
      end
      
      # all peers know transaction state
      l = join(peers, xacts)
      l.map do |p, x|
        results <+ [p.peer, x.xid, x.state]
      end
    }    
  end
end