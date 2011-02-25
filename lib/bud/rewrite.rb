require 'rubygems'
require 'ruby2ruby'

class RW < Ruby2Ruby
  attr_accessor :rule_indx, :rules, :depends

  def initialize(seed)
    @ops = {:<< => 1, :< => 1, :<= => 1}
    @nm_funcs = {:group => 1, :argagg => 1, :include? => 1, :-@ => 1}
    @temp_ops = {:-@ => 1, :~ => 1, :+@ => 1}
    @tables = {}
    @nm = false
    @rule_indx = seed
    @collect = false
    @join_alias = {}
    @rules = []
    @depends = []
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
    lvar = exp.first.to_s
    if @join_alias[lvar]
      @tables[lvar] = @nm
      drain(exp)
      return lvar
    else
      super
    end
  end

  def process_call(exp)
    if exp[0].nil? and exp[2] == s(:arglist) and @collect
      do_table(exp)
    elsif @ops[exp[1]] and self.context[1] == :block
      do_rule(exp)
    else
      # basically not analyzed
      if @nm_funcs[exp[1]]
        @nm = true
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

    @rules << [@rule_indx, lhs, op, rule_txt]
    @tables.each_pair do |k, v|
      @depends << [@rule_indx, lhs, op, k, v]
    end

    @tables = {}
    @nm = false
    @temp_op = nil
    @rule_indx += 1
  end

  def do_table(exp)
    t = exp[1].to_s
    @tables[t] = @nm
    drain(exp)
    return t
  end

  def do_join_alias(exp)
    t = exp[0].to_s
    @join_alias[t] = true
    @tables[t] = @nm
    @collect = true
    rhs = collect_rhs(exp[1])
    @collect = false
    record_rule(t, "=", rhs)
    drain(exp)
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    rhs = collect_rhs(exp[2])
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
  end
end


class StateExtractor < Ruby2Ruby
  attr_reader :decls

  def initialize(context)
    @cxt = context
    @decls = []
    super()
  end

  def process_call(exp)
    lhs = process exp[2]
    foo = "#{exp[1]} #{lhs}"
    @decls << ["#{lhs}"[/:.*?,/][1..-1].chop!, foo]
    exp.shift until exp.empty?
    return ""
  end
end
