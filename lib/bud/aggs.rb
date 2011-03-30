module Bud
  ######## Agg definitions
  class Agg #:nodoc: all
    def init(val)
      val
    end

    def final(state)
      state
    end
  end

  class Exemplary < Agg #:nodoc: all
  end

  # ArgExemplary aggs are used by argagg. Canonical examples are min/min (argmin/max)
  # They must have a trivial final method and be monotonic, i.e. once a value v
  # is discarded in favor of another, v can never be the final result

  class ArgExemplary < Agg #:nodoc: all
    def tie(state, val)
      (state == val)
    end
    def final(state)
      state
    end
  end

  class Min < ArgExemplary #:nodoc: all
    def trans(state, val)
      state < val ? state : val
    end
  end
  # exemplary aggregate method to be used in Bud::BudCollection.group.  
  # computes minimum of x entries aggregated.
  def min(x)
    [Min.new, x]
  end

  class Max < ArgExemplary #:nodoc: all
    def trans(state, val)
      state > val ? state : val
    end
  end
  # exemplary aggregate method to be used in Bud::BudCollection.group.  
  # computes maximum of x entries aggregated.
  def max(x)
    [Max.new, x]
  end

  class Choose < ArgExemplary #:nodoc: all
    def trans(state, val)
      state.nil? ? val : state
    end
    def tie(state, val)
      false
    end
  end

  # exemplary aggregate method to be used in Bud::BudCollection.group.  
  # arbitrarily but deterministically chooses among x entries being aggregated.
  def choose(x)
    [Choose.new, x]
  end

  class Sum < Agg #:nodoc: all
    def trans(state, val)
      state + val
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
    def trans(state, x=nil)
      state + 1
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
    def trans(state, val)
      retval = [state[0] + val]
      retval << (state[1] + 1)
    end
    def final(state)
      state[0]*1.0 / state[1]
    end
  end
  
  # aggregate method to be used in Bud::BudCollection.group.  
  # computes average of a multiset of x values
  def avg(x)
    [Avg.new, x]
  end

  class Accum < Agg #:nodoc: all
    def init(x)
      [x]
    end
    def trans(state, val)
      state << val
    end
  end
  
  # aggregate method to be used in Bud::BudCollection.group.  
  # accumulates all x inputs into an array
  def accum(x)
    [Accum.new, x]
  end
end
