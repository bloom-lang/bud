class Bud
  ######## Agg definitions
  class Agg
    def init (val)
      val
    end

    def final(state)
      state
    end
  end

  class Exemplary < Agg
  end

  # ArgExemplary aggs are used by argagg. Canonical examples are min/min (argmin/max)
  # They must have a trivial final method and be monotonic, i.e. once a value v
  # is discarded in favor of another, v can never be the final result
  
  class ArgExemplary < Agg
    def tie(state, val)
      (state == val )
    end   
    def final(state)
      state
    end
  end
  
  class Min < ArgExemplary
    def trans(state, val)
      state < val ? state : val
    end
  end
  def min(x)
    [Min.new, x]
  end

  class Max < ArgExemplary
    def trans(state, val)
      state > val ? state : val
    end
  end
  def max(x)
    [Max.new, x]
  end
  
  class Choose < ArgExemplary
    def trans(state, val)
      state.nil? ? val : state
    end
    def tie(state, val)
      false
    end
  end
  def choose(x)
    [Choose.new, x]
  end

  class Sum < Agg
    def trans(state, val)
      state + val
    end
  end
  def sum(x)
    [Sum.new, x]
  end

  class Count < Agg
    def init(x=nil)
      1
    end
    def trans(state, x=nil)
      state + 1
    end
  end  
  def count(x=nil)
    [Count.new]
  end  

  class Avg < Agg
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
  def avg(x) 
    [Avg.new, x]
  end
end
