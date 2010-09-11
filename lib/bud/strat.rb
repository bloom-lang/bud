require 'rubygems'
require 'bud'

class Stratification < Bud
  def state
    table :depends, ['head', 'arg', 'body', 'neg']
    table :depends_tc, ['head', 'body', 'neg']
    table :cycle, ['predicate'], ['neg']
    table :stratum_base, ['predicate', 'stratum']
    table :stratum, ['predicate'], ['stratum']
    table :top_strat, ['stratum']
  end
  
  def declaration
    strata[0] = rules {

      depends_tc <= depends.map { |d| [d.head, d.body, d.neg] }
      dj = join [depends, depends_tc], [depends.body, depends_tc.head]
      depends_tc <= dj.map do |b, r|
        if b.neg || r.neg
          [b.head, r.body, true]
        else
          [b.head, r.body, false]
        end
      end

      cycle <= depends_tc.map do |d|
        if d.head == d.body
          [d.head, d.neg]
        end
      end

    }

    strata[1] = rules {
      stratum_base <= depends.map {|d1| [d1.body, 0] unless depends.map{|d2| [d2.head]}.include? d1.body} 
      j = join [depends, stratum_base], [depends.body, stratum_base.predicate] 
      stratum_base <= j.map do |d,s| 
        if d.neg == 1
          #print "#{d.head} <- #{d.body} neg so +\n"
          [d.head, s.stratum + 1]
        else
          [d.head, s.stratum] # unless stratum.map{|s2| [s2.predicate]}.include? d.head 
        end
      end

    }
    strata[2] = rules {
      stratum <= stratum_base.group([stratum_base.predicate], max(stratum_base.stratum))
      top_strat <= stratum_base.group(nil, max(stratum_base.stratum)) 
    }
  end
end

