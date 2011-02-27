require 'bud/rewrite'
require 'bud/state'
require 'parse_tree'

class BudMeta
  include BudState
  attr_reader :depanalysis, :decls

  def initialize(bud_instance, declarations)
    @bud_instance = bud_instance
    @declarations = declarations
    @decls = []
  end

  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)
    shred_rules
    top_stratum = stratify
    stratum_map = binaryrel2map(@bud_instance.t_stratum)

    rewritten_strata = Array.new(top_stratum + 1, "")
    @bud_instance.t_rules.sort{|a, b| oporder(a.op) <=> oporder(b.op)}.each do |d|
      # joins may have to be restated
      belongs_in = stratum_map[d.lhs]
      belongs_in ||= 0
      if d.op == "="
        (belongs_in..top_stratum).each do |i|
          rewritten_strata[i] += "#{d.src}\n"
        end
      else
        rewritten_strata[belongs_in] += "#{d.src}\n"
      end
    end

    @depanalysis = DepAnalysis.new
    @bud_instance.t_depends_tc.each {|d| @depanalysis.depends_tc << d}
    @bud_instance.t_provides.each {|p| @depanalysis.providing << p}
    3.times { @depanalysis.tick }

    @depanalysis.underspecified.each do |u|
      puts "Warning: underspecified dataflow: #{u.inspect}"
    end
    dump_rewrite(rewritten_strata) if @bud_instance.options[:dump_rewrite]

    return rewritten_strata
  end

  def binaryrel2map(rel)
    map = {}
    rel.each do |s|
      raise Bud::BudError unless s.length == 2
      map[s[0]] = s[1]
    end
    return map
  end

  def shred_rules
    # to completely characterize the rules of a bud class we must extract
    # from all parent classes/modules
    # after making this pass, we no longer care about the names of methods.
    # we are shredding down to the granularity of rule heads.
    seed = 0
    rulebag = {}
    each_relevant_ancestor do |anc|
      shred_state(anc)
      @declarations.each do |meth_name|
        rw = rewrite_rule_block(anc, meth_name, seed)
        if rw
          seed = rw.rule_indx
          rulebag[meth_name] = rw
        end
      end
    end

    rulebag.each_value do |v|
      v.rules.each do |r|
        @bud_instance.t_rules << r
      end
      v.depends.each do |d|
        @bud_instance.t_depends << d
      end
    end
  end

  def shred_state(anc)
    return unless @bud_instance.options[:scoping]
    stp = ParseTree.translate(anc, "__#{@bud_instance.class}__state")
    return if stp[0].nil?
    state_reader = StateExtractor.new(anc.to_s)
    u = Unifier.new
    pt = u.process(stp)
    res = state_reader.process(pt)
    @decls += state_reader.decls
  end

  def rewrite_rule_block(klass, block_name, seed)
    parse_tree = ParseTree.translate(klass, block_name)
    return unless parse_tree.first

    u = Unifier.new
    pt = u.process(parse_tree)
    check_rule_ast(pt)

    rewriter = RuleRewriter.new(seed)
    rewriter.process(pt)
    #rewriter.rules.each {|r| puts "RW: #{r.inspect}"}
    return rewriter
  end

  # Perform some basic sanity checks on the AST of a rule block. We expect a
  # rule block to consist of a :defn, a nested :scope, and then a sequence of
  # statements. Each statement is either a :call or :lasgn node.
  def check_rule_ast(pt)
    return if @bud_instance.options[:disable_sanity_check]

    # :defn format: node tag, block name, args, nested scope
    raise Bud::CompileError if pt[0] != :defn
    scope = pt[3]
    raise Bud::CompileError if scope[0] != :scope
    block = scope[1]

    # First, remove any assignment statements (i.e., alias definitions) from the
    # rule block's AST. Then macro-expand any references to the alias in the
    # rest of the rule block.
    assign_nodes, rest_nodes = block.partition {|b| b[0] == :lasgn}
    assign_vars = {}
    assign_nodes.each do |n|
      # Expected format: lasgn tag, lhs, rhs
      raise Bud::CompileError unless n.length == 3
      tag, lhs, rhs = n

      # Don't allow duplicate variable names within a block, nor variables that
      # shadow the name of a collection
      raise Bud::CompileError if assign_vars.has_key? lhs
      raise Bud::CompileError if @bud_instance.tables.has_key? lhs
      assign_vars[lhs] = rhs
    end

    rest_nodes.each_with_index do |n,i|
      if i == 0
        raise Bud::CompileError if n != :block
        next
      end

      raise Bud::CompileError if n[0] != :call
      # Rule format: call tag, lhs, op, rhs
      raise Bud::CompileError unless n.length == 4
      tag, lhs, op, rhs = n

      # Check that LHS references a named collection
      raise Bud::CompileError unless lhs[0] == :call
      lhs_name = lhs[2]
      raise Bud::CompileError unless @bud_instance.tables.has_key? lhs_name.to_sym

      # Check that op is a legal Bloom operator
      raise Bud::CompileError unless [:<, :<=, :<<].include? op

      # Check superator invocation. A superator that begins with "<" is parsed
      # as a call to the binary :< operator. The right operand to :< is a :call
      # node; the LHS of the :call is the actual rule body, the :call's oper is
      # the rest of the superator (unary ~, -, +), and the RHS is empty.  Note
      # that ParseTree encodes unary "-" and "+" as :-@ and :-+, respectively.
      # XXX: Checking for illegal superators (e.g., "<--") is tricky, because
      # they are encoded as a nested unary operator in the rule body.
      if op == :<
        raise Bud::CompileError unless rhs[0] == :arglist
        body = rhs[1]
        raise Bud::CompileError unless body[0] == :call
        op_tail = body[2]
        raise Bud::CompileError unless [:~, :-@, :+@].include? op_tail
        rhs_args = body[3]
        raise Bud::CompileError unless rhs_args[0] == :arglist
        raise Bud::CompileError if rhs_args.length != 1
      end
    end

    # Replace old block with rewritten version
    # scope[1] = rest_nodes
  end

  def each_relevant_ancestor
    on = false
    @bud_instance.class.ancestors.reverse.each do |anc|
      if on
        yield anc
      elsif anc == Bud
        on = true
      end
    end
  end

  def stratify
    strat = Stratification.new
    @bud_instance.tables.each do |t|
      strat.tab_info << [t[0].to_s, t[1].class, t[1].schema.length]
    end
    @bud_instance.t_depends.each do |d|
      strat.depends << d
    end
    strat.tick

    # Copy computed data back into Bud runtime
    strat.stratum.each {|s| @bud_instance.t_stratum << s}
    strat.depends_tc.each {|d| @bud_instance.t_depends_tc << d}
    strat.cycle.each {|c| @bud_instance.t_cycle << c}
    if strat.top_strat.length > 0
      top = strat.top_strat.first.stratum
    else
      top = 1
    end
    return top
  end

  def oporder(op)
    case op
      when '='
        return 0
      when '<<'
        return 1
      when '<='
        return 2
    else
      return 3
    end
  end

  def dump_rewrite(strata)
    fout = File.new("#{@bud_instance.class}_rewritten.txt", "w")
    fout.puts "Declarations:"
    @decls.each do |d|
      fout.puts d
    end

    strata.each_with_index do |r, i|
      fout.puts "R[#{i}]:\n #{r}"
    end
    fout.close
  end
end
