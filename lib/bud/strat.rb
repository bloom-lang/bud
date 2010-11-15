require 'rubygems'
require 'bud'

class Stratification < Bud
  def state
    table :depends, ['rule', 'head', 'op', 'body', 'neg']

    # adding a 'via' attribute for further analysis
    table :depends_tc, ['head', 'body', 'via', 'neg', 'temporal']
    table :cycle, ['predicate', 'via', 'neg', 'temporal']
    table :stratum_base, ['predicate', 'stratum']
    table :stratum, ['predicate', 'stratum']
    table :top_strat, ['stratum']

    table :tab_info, ['tab', 'type', 'columns']

    table :col_alias, ['head', 'als', 'name', 'ord']
    table :tab_alias, ['head', 'tab', 'als']
    scratch :guarded, ['channel', 'table']
  end
  
  def declaration
    strata[0] = rules {
      depends_tc <= depends.map do |d| 
        dneg = (d.neg == 1 or d.op.to_s =~ /<-/)
        if d.op.to_s =~ /<[+-~]/ then
          [d.head, d.body, d.body, dneg, true] 
        else
          [d.head, d.body, d.body, dneg, false] 
        end
      end
      dj = join [depends, depends_tc], [depends.body, depends_tc.head]
      depends_tc <= dj.map do |b, r|
        # theoretically illegal, would break our analysis
        temporal = false
        if (b.op.to_s =~ /<[+-~]/) or r.temporal
          temporal = true
        end
        if (b.neg == 1 or b.op.to_s =~ /<-/) || r.neg  
          # revert the computation of 'via' -- too slow
          # b.body -> nil
          [b.head, r.body, b.body, true, temporal]
        else
          [b.head, r.body, b.body, false, temporal]
        end
      end

      #print "OK CYCLE has #{cycle.length} elements, TC has #{depends_tc.length}!\n"
      #depends_tc.each {|d| print "\tDEP_TC: #{d.inspect}\n" }

      cycle <= depends_tc.map do |d|
        if d.head == d.body
          if d.neg and !d.temporal
            raise RuntimeError.new("unstratifiable program: #{d.inspect}")
          else
            #print "CYC: #{d.inspect}\n"
            [d.head, d.via, d.neg, d.temporal]
            #[d.head, d.body, d.neg, d.temporal]
          end
        end
      end
      stratum_base <= depends.map{|d| [d.body, 0]}
    }

    strata[1] = rules {
      stratum_base <= join([depends, stratum_base], [depends.body, stratum_base.predicate]).map do |d, s|
        #print "do base #{d.inspect}\n"
        if (d.neg == 1 or d.op.to_s == "<-") and !(cycle.map{|c| c.predicate}.include? d.body and cycle.map{|c| c.predicate}.include? d.head)
          #print "BUMP #{d.head} from #{s.stratum}\n"
          [d.head, s.stratum + 1]
        else  
          #print "HOIST #{d.head} #{s.stratum}\n"
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

