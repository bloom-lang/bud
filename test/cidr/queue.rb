require 'rubygems'
require 'bud'

# you could miss it!

class BaseQueue < Bud
  def state
    table :q, ['id', 'payload']
    table :min_id, ['id']
    scratch :head, ['id', 'payload']

    table :presented, ['id']
    #scratch :consumed, ['id']
    table :consumed, ['id']
  end

  declare
    def qlogic
      min_id <= q.group(nil, min(q.id))
    end

  declare 
    def qlogic2
      # Why not use argagg here?  -- JMH
      j = join [q, min_id], [q.id, min_id.id]
      #head <= j.map{|q, m| q unless presented.map{|p| p.id}.include? q.id}
      head <= j.map do |q, m| 
        unless presented.map{|p| p.id}.include? q.id
          #presented.each{|p| print "\tPRES: #{p.inspect}\n"}
          q
        end
      end

      presented <+ head.map{|h| [h.id]}

      j2 = join [q, consumed], [q.id, consumed.id]
      q <- j2.map{|q, c| print "adjust q\n"; q}
    end
  
end


