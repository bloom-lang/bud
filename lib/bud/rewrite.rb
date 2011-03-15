require 'rubygems'
require 'ruby2ruby'

class RuleRewriter < Ruby2Ruby
  attr_accessor :rule_indx, :rules, :depends, :bud_instance

  def initialize(seed, bud_instance)
    @bud_instance = bud_instance
    @ops = {:<< => 1, :< => 1, :<= => 1}
    @monotonic_whitelist = {:== => 1, :+ => 1, :- => 1, :<= => 1, :- => 1, :< => 1, :> => 1}
    @temp_ops = {:-@ => 1, :~ => 1, :+@ => 1}
    @tables = {}
    @nm = false
    @rule_indx = seed
    @collect = false
    @rules = []
    @depends = []
    super()
  end
    
  def process_call(exp)
    if exp[0].nil? and exp[2] == s(:arglist) and @collect
      do_table(exp)
    elsif @ops[exp[1]] and @context[1] == :block and @context.length == 4
      # NB: context.length is 4 when see a method call at the top-level of a
      # :defn block -- this is where we expect Bloom ops to appear
      do_rule(exp)
    else
      if exp[0] and exp[0].class == Sexp
        # ignore accessors of iterator variables, 
        # but do analyze variables from equality rules that got turned into temps!
        if exp[0].first != :lvar or @bud_instance.tables.include? exp[0][1]
          if exp[2].class == Sexp and exp[2].length == 1 and exp[2] == s(:arglist)
            # check for delete ops and predicate methods (ending in "?" like "empty?"), 
            # but ignore top-level accessors and maps
            # XXX we should be more methodical about white/black-listing unary Enumerator 
            # methods, as this will silently fail to notice non-monotonicity if we're wrong.
            @nm = true if exp[1] == :-@ or exp[1].to_s[-1..-1] == '?'
          else
            unless @monotonic_whitelist[exp[1]]
              # suspicious function: exp[1]
              @nm = true
            end
          end
          # now check for variables from equality rules that we converted into temps
          # and register in @tables for dependency checking
          if exp[0].first == :lvar and @bud_instance.tables.include? exp[0][1]
            @tables[exp[0][1].to_s] = @nm
          end
        end
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

  def do_rule(exp)
    if exp[0][2] == :temp
      temp = true
      lhs = handle_temp(exp[0])
    else
      lhs = exp[0]
    end
    lhs = process lhs
    op = exp[1]
    rhs = collect_rhs(map2pro(exp[2]))
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  def handle_temp(lhs)
    bud_instance.temp lhs[3][1][2]
    return lhs[3][1]
  end

  # look for top-level map on a base-table on rhs, and rewrite to pro
  def map2pro(exp)
    if exp[1] and exp[1][0] and exp[1][0] == :iter \
       and exp[1][1] and exp[1][1][1] == :call \
       and exp[1][1][2] == :map
      exp[1][1][2] = :pro 
    end
    exp
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
