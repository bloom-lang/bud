module Bud
  ######## Agg definitions
  class Agg #:nodoc: all
    def init(val)
      val
    end

    # In order to support argagg, trans must return a pair:
    #  1. the running aggregate state
    #  2. a flag to indicate what the caller should do with the input tuple for argaggs
    #     a. :ignore tells the caller to ignore this input
    #     b. :keep tells the caller to save this input
    #     c. :replace tells the caller to keep this input alone
    #     d. :delete, [t1, t2, ...] tells the caller to delete the remaining tuples
    #  For aggs that do not inherit from ArgExemplary, the 2nd part can simply be nil.
    def trans(the_state, val)
      return the_state, :ignore
    end

    def final(the_state)
      the_state
    end
  end

  # ArgExemplary aggs are used by argagg. Canonical examples are min/min (argmin/max)
  # They must have a trivial final method and be monotonic, i.e. once a value v
  # is discarded in favor of another, v can never be the final result.
  class ArgExemplary < Agg #:nodoc: all
    def tie(the_state, val)
      the_state == val
    end
    def final(the_state)
      the_state
    end
  end

  class Min < ArgExemplary #:nodoc: all
    def trans(the_state, val)
      if (the_state <=> val) < 0
        return the_state, :ignore
      elsif the_state == val
        return the_state, :keep
      else
        return val, :replace
      end
    end
  end
  # exemplary aggregate method to be used in Bud::BudCollection.group.
  # computes minimum of x entries aggregated.
  def min(x)
    [Min.new, x]
  end

  class Max < ArgExemplary #:nodoc: all
    def trans(the_state, val)
      if (the_state <=> val) > 0
        return the_state, :ignore
      elsif the_state == val
        return the_state, :keep
      else
        return val, :replace
      end
    end
  end
  # exemplary aggregate method to be used in Bud::BudCollection.group.
  # computes maximum of x entries aggregated.
  def max(x)
    [Max.new, x]
  end

  class BooleanAnd < ArgExemplary #:nodoc: all
    def trans(the_state, val)
      if val == false
        return val, :replace
      else
        return the_state, :ignore
      end
    end
  end

  def bool_and(x)
    [BooleanAnd.new, x]
  end

  class BooleanOr < ArgExemplary #:nodoc: all
    def trans(the_state, val)
      if val == true
        return val, :replace
      else
        return the_state, :ignore
      end
    end
  end

  def bool_or(x)
    [BooleanOr.new, x]
  end

  class Choose < ArgExemplary #:nodoc: all
    def trans(the_state, val)
      if the_state.nil?
        return val, :replace
      else
        return the_state, :ignore
      end
    end
    def tie(the_state, val)
      false
    end
  end

  # exemplary aggregate method to be used in Bud::BudCollection.group.
  # arbitrarily but deterministically chooses among x entries being aggregated.
  def choose(x)
    [Choose.new, x]
  end

  class ChooseOneRand < ArgExemplary #:nodoc: all
    def init(x=nil) # Vitter's reservoir sampling, sample size = 1
      the_state = {:cnt => 1, :val => x}
    end

    def trans(the_state, val)
      the_state[:cnt] += 1
      j = rand(the_state[:cnt])
      if j < 1 # replace
        old_val = the_state[:val]
        the_state[:val] = val
        return the_state, :replace
      else
        return the_state, :ignore
      end
    end
    def tie(the_state, val)
      true
    end
    def final(the_state)
      the_state[:val] # XXX rand(@@reservoir_size will always be 0, since @@reservoir_size is 1
    end
  end

  # exemplary aggregate method to be used in Bud::BudCollection.group.
  # randomly chooses among x entries being aggregated.
  def choose_rand(x=nil)
    [ChooseOneRand.new, x]
  end

  class Sum < Agg #:nodoc: all
    def trans(the_state, val)
      return the_state + val, nil
    end
  end

  # aggregate method to be used in Bud::BudCollection.group.
  # computes sum of x entries aggregated.
  def sum(x)
    [Sum.new, x]
  end

  class Count < Agg #:nodoc: all
    def init(x=nil)
      1
    end
    def trans(the_state, x=nil)
      return the_state + 1, nil
    end
  end

  # aggregate method to be used in Bud::BudCollection.group.
  # counts number of entries aggregated.  argument is ignored.
  def count(x=nil)
    [Count.new]
  end

  class Avg < Agg #:nodoc: all
    def init(val)
      [val, 1]
    end
    def trans(the_state, val)
      retval = [the_state[0] + val]
      retval << (the_state[1] + 1)
      return retval, nil
    end
    def final(the_state)
      the_state[0].to_f / the_state[1]
    end
  end

  # aggregate method to be used in Bud::BudCollection.group.
  # computes average of a multiset of x values
  def avg(x)
    [Avg.new, x]
  end

  class Accum < Agg #:nodoc: all
    def init(x)
      [x].to_set
    end
    def trans(the_state, val)
      the_state << val
      return the_state, nil
    end
  end

  # aggregate method to be used in Bud::BudCollection.group.
  # accumulates all x inputs into a set.
  def accum(x)
    [Accum.new, x]
  end

  class AccumPair < Agg #:nodoc: all
    def init(fst, snd)
      [[fst, snd]].to_set
    end
    def trans(the_state, fst, snd)
      the_state << [fst, snd]
      return the_state, nil
    end
  end

  # aggregate method to be used in Bud::BudCollection.group.
  # accumulates x, y inputs into a set of pairs (two element arrays).
  def accum_pair(x, y)
    [AccumPair.new, x, y]
  end
end
