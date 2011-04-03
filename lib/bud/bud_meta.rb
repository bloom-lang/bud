require 'bud/rewrite'
require 'bud/state'
require 'parse_tree'
require 'pp'

class BudMeta #:nodoc: all
  attr_reader :depanalysis

  def initialize(bud_instance, declarations)
    @bud_instance = bud_instance
    @declarations = declarations
  end

  def meta_rewrite
    shred_rules
    top_stratum = stratify
    stratum_map = binaryrel2map(@bud_instance.t_stratum)

    rewritten_strata = Array.new(top_stratum + 1) { [] }
    @bud_instance.t_rules.each do |d|
      belongs_in = stratum_map[d.lhs]
      belongs_in ||= 0
      rewritten_strata[belongs_in] << d.src
    end

    @depanalysis = DepAnalysis.new
    @bud_instance.t_depends_tc.each {|d| @depanalysis.depends_tc << d}
    @bud_instance.t_provides.each {|p| @depanalysis.providing << p}
    3.times { @depanalysis.tick }

    @depanalysis.underspecified.each do |u|
      puts "Warning: underspecified dataflow: #{u.inspect}"
      @bud_instance.t_underspecified << u
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
    @bud_instance.class.ancestors.reverse.each do |anc|
      @declarations.each do |meth_name|
        rw = rewrite_rule_block(anc, meth_name, seed)
        if rw
          seed = rw.rule_indx
          rulebag[meth_name] = rw
        end
      end
    end

    rulebag.each_value do |v|
      v.rules.each {|r| @bud_instance.t_rules << r}
      v.depends.each {|d| @bud_instance.t_depends << d}
    end
  end

  def rewrite_rule_block(klass, block_name, seed)
    parse_tree = ParseTree.translate(klass, block_name)
    return unless parse_tree.first

    pt = Unifier.new.process(parse_tree)
    pp pt if @bud_instance.options[:dump_ast]
    begin
      check_rule_ast(pt)
    rescue Exception => e
      # try to "generate" the source code associated with the problematic
      # block, so as to generate a more meaningful error message.
      # if this parse fails, return the original exception (not the new one).
      begin
        code = Ruby2Ruby.new.process(pt)
      rescue Exception => sub_e
        raise e, "Error parsing rule block #{block_name}.  Could not extract source."
      end
      raise e, "Error parsing rule block #{block_name}:\n#{code}"
    end

    rewriter = RuleRewriter.new(seed, @bud_instance)
    rewriter.process(pt)
    return rewriter
  end

  # Perform some basic sanity checks on the AST of a rule block. We expect a
  # rule block to consist of a :defn, a nested :scope, and then a sequence of
  # statements. Each statement is a :call node.
  def check_rule_ast(pt)
    # :defn format: node tag, block name, args, nested scope
    raise Bud::CompileError if pt.sexp_type != :defn
    scope = pt[3]
    raise Bud::CompileError if scope.sexp_type != :scope
    block = scope[1]

    block.each_with_index do |n,i|
      if i == 0
        raise Bud::CompileError if n != :block
        next
      end

      raise Bud::CompileError if n.sexp_type != :call
      raise Bud::CompileError unless n.length == 4

      # Rule format: call tag, lhs, op, rhs
      tag, lhs, op, rhs = n

      # Check that LHS references a named collection or is a temp expression
      raise Bud::CompileError if lhs.nil? or lhs.sexp_type != :call
      lhs_name = lhs[2]
      unless @bud_instance.tables.has_key? lhs_name.to_sym
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
      # XXX: We don't check for illegal superators (e.g., "<--"). That would be
      # tricky, because they are encoded as a nested unary op in the rule body.
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

  def dump_rewrite(strata)
    fout = File.new("#{@bud_instance.class}_rewritten.txt", "w")
    fout.puts "Declarations:"

    strata.each_with_index do |src_ary, i|
      text = src_ary.join("\n")
      fout.puts "R[#{i}]:\n#{text}"
    end
    fout.close
  end
end
