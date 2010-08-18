# MyR2R uses Ruby2Ruby (via SaneR2R) to extract the 'rules' component
# of a Bud subclass.  It uses a TabStuff object to store the extracted 
# rules, and ultimately present them in the appropriate order to create
# a rewritten set of blocks.  
# Each rule has a "head" table, a (su|o)perator and a definition.
# Elsewhere, we have associated each head table with a stratum.  For each,
# we want to return the rules sorted by operator: <<, =, rest, to ensure
# that we produce legal ruby inside the rewritten block.

require 'bud/sane_r2r'

class TabStuff 
  attr_reader :strata
  def initialize
    @state = Hash.new
    @order = Hash['<<' => 1, '=' => 2, '<=' => 3, '<+' => 4, '<-' => 5]
    @strata = Array.new
  end

  def add(lhs, op, whole)
    if !@state[lhs] 
      @state[lhs] = Array.new
    end
    @state[lhs] << [op, whole]
  end

  def add_strat(t, s)
    if !@strata[s]  
      @strata[s] = Array.new
    end
    @strata[s] << t
  end

  def op_of(hashrec)
    # better way?
    if @state[hashrec] && @state[hashrec].class == Array && @state[hashrec].length > 0 && @state[hashrec][0].class == Array
      return @state[hashrec][0][0]
    else
      return "="
    end
  end

  def order_of(rec)
    if @order[rec] 
      @order[rec]
    else 
      # some high number :)
      6
    end
  end

  def strata_each(s)
    @strata[s].sort{|a, b| order_of(op_of(a)) <=> order_of(op_of(b))}.each do |tab|
      if @state[tab] && @state[tab].class == Array
        @state[tab].each do |rec|
          yield rec[1]
        end
      end
    end
  end
end

class MyR2R < SaneR2R
  attr_reader :tabstuff

  def initialize
    @strat = false
    @ops = Hash['<=', 1, '<-', 1, '<+', 1, '<<', 1]
    @expops = Hash['+', 1, '-', 1, '*', 1, '/', 1, '==', 1]
    @aggs = Hash['max', 1, 'min', 1, 'sum', 1, 'avg', 1]
    @grpfuncs = Hash['group', 1, 'argagg', 1]
    @tabstuff = TabStuff.new
    @curr = false
    super
  end

  def process_block(exp)
    if @strat 
      # deal with the clauses one by one.
      until exp.empty?
        term = exp.shift
        fst = term.first.to_s
        if fst == "call" or fst == "dasgn_curr"
          # we are just side-effecting here
          process term
        else
          raise "Invalid top-level clause: \"#{fst}\" in \"#{(process term).to_s}\""
        end
      end 
      return ""
    else
      super exp
    end
  end

  def process_array(exp)
    if @curr 
      return "#{process_arglist(exp)}"
    else
      return "[#{process_arglist(exp)}]"
    end
  end

  def process_attrasgn(exp)
    s = process exp[0].clone
    if s == "strata" 
      @strat = true
      ret = super 
      @strat = false
      return ret
    else
      return super
    end
  end

  def process_call(exp)
    if @strat
      if @ops[exp[1].to_s]
        lhs = (process exp.shift).to_s
        op = exp.shift
        rhs = trim_brackets(process exp.shift)
        @tabstuff.add(lhs, op, lhs + ' ' + op.to_s + ' ' + rhs)
        ret =  ""
      elsif @expops[exp[1].to_s] 
        # don't like this approach
        oldcurr = @curr
        @curr = true
        ret = super(exp)
        @curr = false unless oldcurr
      elsif @grpfuncs[exp[1].to_s]
        # more hacks
        func = exp[1].to_s
        l = process exp.shift
        o = exp.shift
        r = trim_brackets(process exp.shift)
        ret = l.to_s + '.' + func  + '(' + r + ')'
      else
        ret = super(exp)
      end
    else
      ret = super(exp)
    end
    return ret
  end

  def process_dasgn_curr(exp)
    if @strat && exp.length == 2
      l = exp.shift
      r = process exp.shift
      @tabstuff.add(l.to_s, "=", l.to_s + " = " + r)
      ret = ""
    else
      ret = super
    end
    return ret
  end 

  def process_masgn(exp)
    oldcurr = @curr
    @curr = true
    ret = super
    @curr = false unless oldcurr
    return ret
  end

  # Helpers
  def trim_brackets(str)
    return str.sub(/^\s*\[/, "").gsub(/\]\s*\z/, "\n")
  end
end
