require 'rubygems'
require 'bud'

# you could miss it!

class BaseQueue < Bud
  def state
    table :q, ['id', 'payload']
    table :min_id, ['id']
    scratch :head, ['id', 'payload']

    table :presented, ['id']
    table :consumed, ['id']
  end

  declare
    def qlogic
      min_id <= q.group(nil, min(q.id))
    end

  declare 
    def qlogic2
      # Why not use argagg here?  -- JMH
      head <= join([q, min_id], [q.id, min_id.id]).map do |q, m| 
        unless presented.map{|p| p.id}.include? q.id
          q
        end
      end

      presented <+ head.map{|h| [h.id]}
      q <- join([q, consumed], [q.id, consumed.id]).map{|q, c| q}
    end
  
end


