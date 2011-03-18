require 'bud/rewrite'
require 'bud/state'
require 'parse_tree'
require 'pp'

class BudMeta
  attr_reader :depanalysis, :decls, :bud_instance

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
      belongs_in = stratum_map[d.lhs]
      belongs_in ||= 0
      rewritten_strata[belongs_in] += "#{d.src}\n"
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
    u = Unifier.new
    pt = u.process(stp)
    state_reader = StateExtractor.new(anc.to_s)
    state_reader.process(pt)
    @decls += state_reader.decls
  end

  def rewrite_rule_block(klass, block_name, seed)
    parse_tree = ParseTree.translate(klass, block_name)
    return unless parse_tree.first

    u = Unifier.new
    pt = u.process(parse_tree)
    pp pt if @bud_instance.options[:dump_ast]
    check_rule_ast(pt)

    rewriter = RuleRewriter.new(seed, bud_instance)
    rewriter.process(pt)
    #rewriter.rules.each {|r| puts "RW: #{r.inspect}"}
    return rewriter
  end
  
  # given a rule of the form "temp <lhs> <op> <rhs>"
  # this is actually a call to "temp" with args "[<lhs> <op> <rhs>]" which 
  # isn't what we mean.
  # So register the temp, and return a rule of the form "<lhs> <op> <rhs>".
  def declare_and_unwrap_temp(n)
    raise Bud::CompileError, "lhs of temp rule not a symbol" if n[3][1][1][0] != :lit
    # temp rules w/o parens on lhs are nested one level down, nil, temp, (call tag, lhs, op, rhs)
    lhs = s(:call, nil, n[3][1][1][1], s(:arglist))
    op = n[3][1][2]
    rhs = n[3][1][3]
    bud_instance.temp n[3][1][1][1]
    
    return s(:call, lhs, op, rhs)
  end    

  # Perform some basic sanity checks on the AST of a rule block. We expect a
  # rule block to consist of a :defn, a nested :scope, and then a sequence of
  # statements. Each statement is either a :call or :lasgn node.
  def check_rule_ast(pt)
    # :defn format: node tag, block name, args, nested scope
    raise Bud::CompileError if pt.sexp_type != :defn
    scope = pt[3]
    raise Bud::CompileError if scope.sexp_type != :scope
    block = scope[1]

    # First, remove any equality statements (i.e., alias definitions) from the
    # rule block's AST. Then convert them to temp rules so we can add them back in.
    assign_nodes, rest_nodes = block.partition {|b| b.class == Sexp && b.sexp_type == :lasgn}
    assign_vars = {}
    equi_rules = []     # equality statements rewritten as temp rules
    assign_nodes.each do |n|
      # Expected format: lasgn tag, lhs, rhs
      raise Bud::CompileError unless n.length == 3
      tag, lhs, rhs = n
      lhs = lhs.to_sym
      bud_instance.temp lhs

      equi_rules << s(:call, s(:call, nil, lhs, s(:arglist)), :<=, s(:arglist, rhs))               
    end

    rest_nodes += equi_rules

    rest_nodes.each_with_index do |n,i|
      if i == 0
        raise Bud::CompileError if n != :block
        next
      end

      raise Bud::CompileError if n.sexp_type != :call
      raise Bud::CompileError unless n.length == 4

      if n[2] == :temp 
        n = declare_and_unwrap_temp(n)
        rest_nodes[i] = n
      end

      # Rule format: call tag, lhs, op, rhs
      tag, lhs, op, rhs = n

      # Check that LHS references a named collection or is a temp expression
      raise Bud::CompileError if lhs.nil? or lhs.sexp_type != :call
      lhs_name = lhs[2]
      unless lhs_name == :temp or @bud_instance.tables.has_key? lhs_name.to_sym
        raise Bud::CompileError, "Table does not exist: '#{lhs_name}'"
      end

      # Check that op is a legal Bloom operator
      unless [:<, :<=, :<<].include? op
        raise Bud::CompileError, "Illegal operator: '#{op}'"
      end

      # Check superator invocation. A superator that begins with "<" is parsed
      # as a call to the binary :< operator. The right operand to :< is a :call
      # node; the LHS of the :call is the actual rule body, the :call's oper is
      # the rest of the superator (unary ~, -, +), and the RHS is empty.  Note
      # that ParseTree encodes unary "-" and "+" as :-@ and :-+, respectively.
      # XXX: Checking for illegal superators (e.g., "<--") is tricky, because
      # they are encoded as a nested unary operator in the rule body.
      if op == :<
        raise Bud::CompileError unless rhs.sexp_type == :arglist
        body = rhs[1]
        raise Bud::CompileError unless body.sexp_type == :call
        op_tail = body[2]
        raise Bud::CompileError unless [:~, :-@, :+@].include? op_tail
        rhs_args = body[3]
        raise Bud::CompileError unless rhs_args.sexp_type == :arglist
        raise Bud::CompileError if rhs_args.length != 1
      end
    end

    # Replace old block with rewritten version
    scope[1] = rest_nodes
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
      strat.tab_info << [t[0].to_s, t[1].class, (t[1].schema.nil? ? 0 : t[1].schema.length)]
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
