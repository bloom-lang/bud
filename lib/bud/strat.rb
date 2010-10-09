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
      stratum_base <= depends.map{|d| [d.body, 0]}
    }

    strata[1] = rules {
      stratum_base <= join([depends, stratum_base], [depends.body, stratum_base.predicate]).map do |d, s|
        if (d.neg == 1 or d.op.to_s == "<-") and !(cycle.map{|c| c.predicate if c.temporal}.include? d.body and cycle.map{|c| c.predicate if c.temporal}.include? d.head)
          [d.head, s.stratum + 1]
        else    
          [d.head, s.stratum]
        end
      end
    }

    strata[2] = rules {
      stratum <= stratum_base.group([stratum_base.predicate], max(stratum_base.stratum))
      top_strat <= stratum_base.group(nil, max(stratum_base.stratum)) 
    }
  end
end

