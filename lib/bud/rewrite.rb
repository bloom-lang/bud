require 'rubygems'
require 'ruby2ruby'

class RuleRewriter < Ruby2Ruby
  attr_accessor :rule_indx, :rules, :depends

  def initialize(seed)
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
        # ignore accessors of iterator variables
        unless exp[0].first == :lvar
          if exp[2].class == Sexp and exp[2].length == 1 and exp[2] == s(:arglist)
            # check for delete op, but ignore top-level accessors and maps
            @nm = true if exp[1] == :-@
          else
            unless @monotonic_whitelist[exp[1]]
              # suspicious function: exp[1]
              @nm = true
            end
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

# Perform macro expansion on a tree of Sexps.
class VarRewriter < SexpProcessor
  def initialize(var_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @var_tbl = var_tbl
  end

  # Return a list of all variables k such that k appears in a "s(:lasgn, :k)"
  # node in the given Sexp tree.
  def find_lasgn_vars(e)
    return [] if e.nil?

    if e.sexp_type == :lasgn
      return [e.sexp_body.first]
    end

    rv = []
    e.sexp_body.each do |s|
      if Array === s
        rv.concat(find_lasgn_vars s)
      end
    end
    rv
end

  # Check whether any of the variables introduced in this block shadow a
  # variable we are rewriting. If so, temporarily stop rewriting that variable,
  # process the block, and then re-add the variable to the rewriting table.
  def process_iter(exp)
    tag, iter, args, body = exp

    shadow_vars = {}
    vars = find_lasgn_vars(args)
    vars.each do |v|
      if @var_tbl.has_key? v
        shadow_vars[v] = @var_tbl.delete(v)
      end
    end

    result = []
    until exp.empty?
      sub_exp = exp.shift
      if Array === sub_exp
        result << process(sub_exp)
      else
        result << sub_exp
      end
    end

    # assert: no duplicate keys inserted after recursion
    raise Bud::CompileError unless (@var_tbl.keys & shadow_vars.keys).empty?
    @var_tbl.merge! shadow_vars

    Sexp.from_array(result)
  end

  def process_lvar(exp)
    var_name = exp[1]
    if @var_tbl.has_key? var_name
      expansion = @var_tbl[var_name]
      # NB: We need to return a deep copy of the macro expansion. This is
      # because subsequent sexp processing is destructive -- we don't want
      # mutations performed to one expansion of a macro to effect other uses of
      # the macro. Apparently this is the best way to do a deep copy in Ruby.
      return Marshal.load(Marshal.dump(expansion))
    end

    return exp
  end
end

module ModuleRewriter
  def self.do_import(mod, local_name)
    raise Bud::BudError unless (mod.class <= Module and local_name.class <= Symbol)
    unless mod <= Bud::BudModule
      raise Bud::BudError, "Imported modules must include BudModule"
    end

    ast = get_module_ast(mod)
    # ...
    return mod
  end

  def get_module_ast(mod)
    u = Unifier.new
    u.process(ParseTree.translate(mod))
  end
end
