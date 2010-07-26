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

  class Min < Exemplary
    def trans(state, val)
      state < val ? state : val
    end
  end
  def min(x)
    [Min.new, x]
  end

  class Max < Exemplary
    def trans(state, val)
      state > val ? state : val
    end
  end
  def max(x)
    [Max.new, x]
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
    def init
      1
    end
    def trans(state)
      state + 1
    end
  end  
  def count
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
