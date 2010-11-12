require 'rubygems'
require 'bud'

require 'lib/reliable_delivery'

#class QuorumDelivery < ReliableDelivery
module QuorumDelivery
  include ReliableDelivery
  include Anise
  annotator :declare


  def state
    super
    table :replies, ['src', 'dst', 'reqid']
    table :reply_cnt, ['src', 'reqid', 'cnt']
    table :member_cnt, ['cnt']

    table :qdmember, ['peer']
  end
  

  declare
    def pepper
      pj = join [pipe, qdmember]
      # if we get a pipe, we should replicate it to all quorum members (but ourselves
      pipe <= pj.map do |p, m| 
        #if m.peer != @addy
          [m.peer, @addy, p.id, p.payload]
        #end
      end
    end

  declare 
    def done
      #ack.each{|a| "GOT an ack named #{a.inspect}\n"}
      j = join [ack, pipe], [ack.id, pipe.id]
      replies <= j.map do |a, p| 
        #print "GOT a reply from #{a.dst} at #{a.src}\n"; 
        [a.src, a.dst, p.id] 
      end
      #reply_cnt <= replies.group([replies.src, replies.reqid], count(replies.dst))
      member_cnt <= qdmember.group(nil, count(qdmember.peer))
    end

  declare
    def alfinal
      reply_cnt <= replies.group([replies.src, replies.reqid], count(replies.dst))
      #reply_cnt.each {|r| print "RC: #{r.inspect}\n"}

      #fj = join [reply_cnt, member_cnt, pipe], [reply_cnt.cnt, member_cnt.cnt], [reply_cnt.reqid, pipe.id]
      fj = join [reply_cnt, member_cnt, pipe], [reply_cnt.reqid, pipe.id]
      pipe_out <= fj.map do |r, m, p| 
        #print "I GOT IT #{m.cnt}, #{r.cnt}, #{p.inspect}\n"
        if m.cnt == r.cnt
          p
        end
      end
    end
end


