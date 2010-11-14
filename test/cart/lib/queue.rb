require 'rubygems'
require 'bud'

# you could miss it!
module BaseQueue
  include Anise
  annotator :declare

  def state
    super
    table :q, ['ident'], ['payload']
    table :min_ident, ['ident']
    scratch :head, ['ident'], ['payload']

    table :presented, ['ident']
    table :consumed, ['ident']
  end

  declare
    def qlogic
      min_ident <= q.group(nil, min(q.ident))
    end

  declare 
    def qlogic2
      # Why not use argagg here?  -- JMH
      head <= join([q, min_ident], [q.ident, min_ident.ident]).map do |q, m| 
        unless presented.map{|p| p.ident}.include? q.ident
          # debug style
          q
        end
      end

      presented <+ head.map{|h| [h.ident]}
      q <- join([q, consumed], [q.ident, consumed.ident]).map{|q, c| q }

    end
  
end


