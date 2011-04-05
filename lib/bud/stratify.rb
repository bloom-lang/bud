require 'rubygems'
require 'bud'

class Stratification # :nodoc: all
  include Bud

  state do
    # Data inserted by client (Bud rewrite code)
    table :tab_info, [:tab, :typecol, :columns]
    table :depends, [:rule, :head, :op, :body, :neg]
    table :depends_clean, [:head, :body, :neg, :temporal]

    # Transitive closure of "depends" relation
    table :depends_tc, [:head, :body, :via, :neg, :temporal]
    table :cycle, [:predicate, :via, :neg, :temporal]
    table :stratum_base, [:predicate, :stratum]
    table :stratum, [:predicate, :stratum]
    table :top_strat, [:stratum]
  end

  def declaration
    strata[0] = lambda {
      depends_clean <= depends do |d|
        is_temporal = (d.op.to_s =~ /<[\+\-\~]/)
        [d.head, d.body, d.neg, is_temporal]
      end

      # Compute the transitive closure of "depends_clean" to detect cycles in
      # the deductive fragment of the program.
      depends_tc <= depends_clean do |d|
        [d.head, d.body, d.body, d.neg, d.temporal]
      end
      depends_tc <= (depends_clean * depends_tc).pairs(:body => :head) do |b, r|
        [b.head, r.body, b.body, (b.neg or r.neg), (b.temporal or r.temporal)]
      end

      cycle <= depends_tc do |d|
        if d.head == d.body
          if d.neg and !d.temporal
            raise Bud::CompileError, "unstratifiable program: #{d.inspect}"
          else
            [d.head, d.via, d.neg, d.temporal]
          end
        end
      end
    
      # we initially assign all predicates to stratum 0
      stratum_base <= depends {|d| [d.body, 0]}
    }

    strata[1] = lambda {
      # classic stratification:
      # if A depends on B, A is >= B.
      # if A depends nonmonotonically on B, A > B.
      # if A are B are co-dependent, give up. 
      #  (don't need to do this, b/c we've ruled out deductive cycles)
      # stratum choice will represent local evaluation order,
      # so we need only consider 'synchronous' dependencies (<=)

      # pass 1: assume no deductive cycles
      # do "vanilla stratification" on deductive rules
      stratum_base <= (depends * stratum_base).pairs(:body => :predicate) do |d, s|
        if d.op.to_s == '<='
          if d.neg
            # BUMP
            [d.head, s.stratum + 1]
          else
            # HOIST
            [d.head, s.stratum]
          end
        end
      end
    }

    strata[2] = lambda {
      # pass 2: do additional hoisting to ensure that rules with scratches in their
      # LHS do not appear in strata below rules that produce their inputs
      stratum_base <= (depends * stratum_base).pairs(:body => :predicate) do |d, s|
        unless d.op.to_s == '<='
          [d.head, s.stratum]
        end
      end
    }

    strata[3] = lambda {
      stratum <= stratum_base.group([stratum_base.predicate], max(stratum_base.stratum))
    }

    strata[4] = lambda {
      # there is no good reason that top_strat can't be computed in strata[3] over stratum_base.
      # however, when it is deduced that way, it is empty after a tick
      top_strat <= stratum.group([], max(stratum.stratum))
    }
  end
end
