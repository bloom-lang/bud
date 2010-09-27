require 'rubygems'
require 'bud'

class Stratification < Bud
  def state
    #table :depends, ['head', 'op', 'body', 'neg']
    table :depends, ['head', 'op', 'body', 'neg']

    # adding a 'via' attribute for further analysis
    table :depends_tc, ['head', 'body', 'via', 'neg', 'temporal']
    #table :cycle, ['predicate'], ['neg']
    table :cycle, ['predicate', 'via', 'neg', 'temporal']
    table :stratum_base, ['predicate', 'stratum']
    #table :stratum, ['predicate'], ['stratum']
    table :stratum, ['predicate', 'stratum']
    table :top_strat, ['stratum']

    table :tab_info, ['tab', 'type', 'columns']

    table :col_alias, ['head', 'als', 'name', 'ord']
    #table :max_col_alias, ['head', 'als', 'name', 'maxord']
    table :tab_alias, ['head', 'tab', 'als']
    scratch :guarded, ['channel', 'table']
  end
  
  def declaration
    strata[0] = rules {
      depends_tc <= depends.map do |d| 
        if d.op.to_s =~ /<[+-]/ then
          [d.head, d.body, d.body, d.neg, true] 
        else
          [d.head, d.body, d.body, d.neg, false] 
        end
      end
      dj = join [depends, depends_tc], [depends.body, depends_tc.head]
      depends_tc <= dj.map do |b, r|
        # theoretically illegal, would break our analysis
        temporal = false
        if (b.op.to_s =~ /<[+-]/) or r.temporal
          temporal = true
        end
        if b.neg || r.neg
          [b.head, r.body, b.body, true, temporal]
        else
          [b.head, r.body, b.body, false, temporal]
        end
      end

      cycle <= depends_tc.map do |d|
        if d.head == d.body
          [d.head, d.via, d.neg, d.temporal]
        end
      end
  
      # loj
      #tab_info <= depends.map{|d| [d.head, 'none', -1] unless tab_info.map{|t| t.tab}.include? d.head}
      tab_info <= depends.map do |d| 
        unless tab_info.map{|t| t.tab}.include? d.head
          [d.head, 'none', -1] 
        else
        end
      end


      stratum_base <= depends.map{|d| [d.body, 0]}

    }

    strata[1] = rules {
      # first principles: what does it mean to be "guarded" ?
      # before it participates in a join, the data coming in
      # this channel is persisted.    
      # because we don't have a meta-abstraction for 'rules',
      # for CIDR we'll insist that a channel is "immediately guarded"
      guarded <= join([depends, tab_info], [depends.head, tab_info.tab]).map do |d, t|
        if t.type == Bud::BudTable
          unless depends.map{|d| d.head if d.op.to_s == "<-"}.include? d.head
            [d.body, d.head]
          end
        end
      end

    }

    strata[2] = rules {
      j = join [depends, stratum_base, tab_info], [depends.body, stratum_base.predicate], [depends.head, tab_info.tab]
      stratum_base <= j.map do |d, s, t| 
        #print "FARQ #{d.inspect}, #{s.inspect}, #{t.inspect}\n"
        if ((d.neg == 1 or d.op.to_s == "<-") or (d.op.to_s == '<+' and t.type == Bud::BudChannel and !guarded.map{|g| g.channel}.include? d.head)) and !(cycle.map{|c| c.predicate if c.temporal}.include? d.body and cycle.map{|c| c.predicate if c.temporal}.include? d.head)
         # print "\tBUMP #{d.head} due to FARQ #{d.inspect}, #{s.inspect}, #{t.inspect}\n"
          
          [d.head, s.stratum + 1]
        else 
          [d.head, s.stratum]
        end
      end

    }
    strata[3] = rules {
      stratum <= stratum_base.group([stratum_base.predicate], max(stratum_base.stratum))
      top_strat <= stratum_base.group(nil, max(stratum_base.stratum)) 
    }
  end
end

