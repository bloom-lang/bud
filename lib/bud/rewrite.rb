require 'rubygems'

require 'bud/sane_r2r'
require 'parse_tree'

class RW < Ruby2Ruby

  attr_accessor :rule_indx

  def initialize(seed)
    @ops = {:<< => 1, :< => 1, :<= => 1 }
    @nm_funcs = {:group => 1, :argagg => 1, :include? => 1, :-@ => 1}
    @temp_ops = {:-@ => 1, :~ => 1, :+@ => 1}
    @tabs = {}
    # for upstream compatibility.  consider using a bool
    @nm = 0
    @rule_indx = seed
    @collect = false
    @delete = false
    @join_alias = {}
    @flat_state = []
    super()
  end

  def process_lasgn(exp)
    if exp.length == 2
      do_join_alias(exp)
    else
      super
    end
  end
    
  def process_lvar(exp)
    lvar = exp[0].to_s
    if @join_alias[lvar]
      @tabs[lvar] = @nm
      drain(exp)
      return lvar
    else
      super
    end
  end

  def process_call(exp)
    if exp[0].nil? and exp[2] == s(:arglist) and @collect
      do_tab(exp)
    elsif @ops[exp[1]] and self.context[1] == :block
      do_rule(exp)
    else
      # basically not analyzed
      if @nm_funcs[exp[1]]
        @nm = 1
      end
      if @temp_ops[exp[1]]
          @temp_op = exp[1].to_s.gsub("@", "")
      end
      super
    end
  end
  
  def collect_rhs(exp)
    @collect = true
    rhs = process exp
    @collect = false
    return rhs
  end

  def record_rule(lhs, op, rhs)
    rule_txt = "#{lhs} #{op} #{rhs}"
    if op == :< 
      op = "<#{@temp_op}"
    else
      op = op.to_s
    end

    @flat_state << [@rule_indx, lhs, op, nil, nil, rule_txt] if @tabs.empty?
    @tabs.each_pair do |k, v| 
      @flat_state << [@rule_indx, lhs, op, k, v, rule_txt]
    end
    @tabs = {}
    @nm = 0
    @temp_op = nil
    @rule_indx += 1
  end

  def do_tab(exp)
    tab = exp[1].to_s
    @tabs[tab] = @nm
    drain(exp)
    return tab
  end

  def do_join_alias(exp)
    tab = exp[0].to_s
    @join_alias[tab] = true
    @tabs[tab] = @nm
    @collect = true
    rhs = collect_rhs(exp[1])
    @collect = false
    record_rule(tab, "=", rhs)
    drain(exp)
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    rhs = collect_rhs(exp[2])
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  def each
    @flat_state.each {|f| yield f }
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
  end
end


class StateExtractor < Ruby2Ruby
  attr_reader :tabs

  def initialize(context)
    @cxt = context
    @tabs = {}
    @ttype = nil
    super()
  end

  def process_zsuper(exp)
    # suppress superclass calls for this rewrite.
    # but consider obsoleting such calls via rewriting!
    exp.shift
    return ""
  end

  def process_block(exp)
    term  = exp.shift
    res = ""
    until term.nil?
      res += process(term) + "\n"
      term = exp.shift
    end
    return res
  end

  def process_defn(exp)
    if exp.first.to_s == "state"
      exp.shift
      ret = exp.shift
      process ret
    end
  end

  def process_vcall(exp)
    @ttype = exp.to_s
    super
  end

  def process_lit(exp)
    if exp.first.class == Symbol
      tab = exp.shift.to_s
      if @cxt.nil?
        res = tab
      else
        res =  @cxt.downcase + "_" + tab
      end
      @tabs[tab] = [res, @ttype]
      return ":" + res
    else
      super
    end
  end
end
