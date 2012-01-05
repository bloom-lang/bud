require 'bud/rewrite'
require 'pp'

class BudMeta #:nodoc: all
  def initialize(bud_instance, declarations)
    @bud_instance = bud_instance
    @declarations = declarations
  end

  def meta_rewrite
    shred_rules # capture dependencies, rewrite rules

    stratified_rules = []
    if @bud_instance.toplevel == @bud_instance
      nodes, stratum_map, top_stratum = stratify_preds

      # stratum_map = {fully qualified pred  => stratum}

      #slot each rule into the stratum corresponding to its lhs pred (from stratum_map)
      stratified_rules = Array.new(top_stratum + 2) { [] }  # stratum -> [ rules ]
      @bud_instance.t_rules.each do |rule|
        if rule.op.to_s == '<='
          # Deductive rules are assigned to strata based on the basic Datalog
          # stratification algorithm
          belongs_in = stratum_map[rule.lhs]
          belongs_in ||= 0
          stratified_rules[belongs_in] << rule
        else
          # All temporal rules are placed in the last stratum
          stratified_rules[top_stratum + 1] << rule
        end
      end
      # stratified_rules[0] may be empty if none of the nodes at stratum 0 are on the lhs
      # stratified_rules[top_stratum+1] will be empty if there are no temporal rules.
      # Cleanup
      stratified_rules = stratified_rules.reject{|r| r.empty?}
      dump_rewrite(stratified_rules) if @bud_instance.options[:dump_rewrite]

    end
    return stratified_rules
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
    return unless klass.respond_to? :__bloom_asts__


    pt = klass.__bloom_asts__[block_name]
    return if pt.nil?

    pt = Marshal.load(Marshal.dump(pt)) #deep clone because RuleRewriter mucks up pt.
    pp pt if @bud_instance.options[:dump_ast]
    tmp_expander = TempExpander.new
    pt = tmp_expander.process(pt)
    tmp_expander.tmp_tables.each do |t|
      @bud_instance.temp(t.to_sym)
    end

    rv = check_rule_ast(pt)
    unless rv.nil?
      if rv.class <= Sexp
        error_pt = rv
        error_msg = "Parse error"
      else
        error_pt, error_msg = rv
      end

      # try to "generate" the source code associated with the problematic block,
      # so as to generate a more meaningful error message.
      begin
        code = Ruby2Ruby.new.process(Marshal.load(Marshal.dump(error_pt)))
        src_msg = "\nCode: #{code}"
      rescue Exception
        src_msg = ""
      end
      raise Bud::CompileError, "#{error_msg} in rule block \"#{block_name}\"#{src_msg}"
    end
    rewriter = RuleRewriter.new(seed, @bud_instance)
    rewriter.process(pt)
    return rewriter
  end

  def get_qual_name(pt)
    # expect to see a parse tree corresponding to a dotted name
    #    a.b.c == s(:call, s1, :c, (:args))
    # where s1 == s(:call, s2, :b, (:args))
    # where s2 == s(:call, nil,:a, (:args))

    tag, recv, name, args = pt
    return nil unless tag == :call or args.length == 1

    if recv
      qn = get_qual_name(recv)
      return nil if qn.nil? or qn.size == 0
      qn = qn + "." + name.to_s
    else
      qn = name.to_s
    end
    qn
  end

  # Perform some basic sanity checks on the AST of a rule block. We expect a
  # rule block to consist of a :defn, a nested :scope, and then a sequence of
  # statements. Each statement is a :call node. Returns nil (no error found), a
  # Sexp (containing an error), or a pair of [Sexp, error message].
  def check_rule_ast(pt)
    # :defn format: node tag, block name, args, nested scope
    return pt if pt.sexp_type != :defn
    scope = pt[3]
    return pt if scope.sexp_type != :scope
    block = scope[1]

    block.each_with_index do |n,i|
      if i == 0
        return pt if n != :block
        next
      end

      # Check for a common case
      if n.sexp_type == :lasgn
        return [n, "Illegal operator: '='"]
      end
      return pt unless n.sexp_type == :call and n.length == 4

      # Rule format: call tag, lhs, op, rhs
      tag, lhs, op, rhs = n

      # Check that LHS references a named collection
      lhs_name = get_qual_name(lhs)
      unless lhs_name and @bud_instance.tables.has_key? lhs_name.to_sym
        return [n, "Table does not exist: '#{lhs_name}'"]
      end

      return [n, "Illegal operator: '#{op}'"] unless [:<, :<=].include? op

      # Check superator invocation. A superator that begins with "<" is parsed
      # as a call to the binary :< operator. The right operand to :< is a :call
      # node; the LHS of the :call is the actual rule body, the :call's oper is
      # the rest of the superator (unary ~, -, +), and the RHS is empty.  Note
      # that ParseTree encodes unary "-" and "+" as :-@ and :-+, respectively.
      # XXX: We don't check for illegal superators (e.g., "<--"). That would be
      # tricky, because they are encoded as a nested unary op in the rule body.
      if op == :<
        return n unless rhs.sexp_type == :arglist
        body = rhs[1]
        return n unless body.sexp_type == :call
        op_tail = body[2]
        return n unless [:~, :-@, :+@].include? op_tail
        rhs_args = body[3]
        return n if rhs_args.sexp_type != :arglist or rhs_args.length != 1
      end
    end

    return nil # No errors found
  end


  Node = Struct.new :name, :status, :stratum, :edges, :in_lhs, :in_body, :in_cycle, :is_neg_head
  # Node.status is one of :init, :in_progress, :done
  Edge = Struct.new :to, :op, :neg, :temporal

  def stratify_preds
    bud = @bud_instance.toplevel
    nodes = {}
    bud.t_depends.each do |d|
      #t_depends [:bud_instance, :rule_id, :lhs, :op, :body] => [:nm]
      lhs = (nodes[d.lhs.to_s] ||= Node.new(d.lhs.to_s, :init, 0, [], true, false, false, false))
      lhs.in_lhs = true
      body = (nodes[d.body.to_s] ||= Node.new(d.body.to_s, :init, 0, [], false, true, false))
      temporal = d.op != "<=" 
      lhs.edges << Edge.new(body, d.op, d.nm, temporal)
      body.in_body = true
    end

    nodes.values.each {|n| calc_stratum(n, false, false, [n.name])}
    # Normalize stratum numbers because they may not be 0-based or consecutive
    remap = {}
    # if the nodes stratum numbers are [2, 3, 2, 4], remap = {2 => 0, 3 => 1, 4 => 2} 
    nodes.values.map {|n| n.stratum}.uniq.sort.each_with_index{|num, i|
      remap[num] = i
    }
    stratum_map = {}
    top_stratum = -1
    nodes.each_pair do |name, n|
      n.stratum = remap[n.stratum]
      stratum_map[n.name] = n.stratum
      top_stratum = max(top_stratum, n.stratum)
    end
    analyze_dependencies(nodes)
    return nodes, stratum_map, top_stratum
  end

  def max(a, b) ; a > b ? a : b ; end

  def calc_stratum(node, neg, temporal, path)
    if node.status == :in_process
      node.in_cycle = true
      if neg and !temporal and node.is_neg_head
        raise Bud::CompileError, "unstratifiable program: #{path.uniq.join(',')}"
      end
    elsif node.status == :init
      node.status = :in_process
      node.edges.each do |edge|
        node.is_neg_head = edge.neg
        next if edge.op != "<="
        body_stratum = calc_stratum(edge.to, (neg or edge.neg), (edge.temporal or temporal), path + [edge.to.name])
        node.is_neg_head = false #reset for next edge
        node.stratum = max(node.stratum, body_stratum + (edge.neg ? 1 : 0))
      end
      node.status = :done
    end
    node.stratum
  end


  def analyze_dependencies(nodes)  # nodes = {node name => node}
    bud = @bud_instance
                                                      
    preds_in_lhs = nodes.inject(Set.new) {|preds, name_n| preds.add(name_n[0]) if name_n[1].in_lhs; preds}
    preds_in_body = nodes.inject(Set.new) {|preds, name_n| preds.add(name_n[0]) if name_n[1].in_body; preds}

    bud.t_provides.each do |p|
      pred, input = p.interface, p.input
      if input
        # an interface pred is a source if it is an input and it is not in any rule's lhs
        #bud.sources << [pred] unless (preds_in_lhs.include? pred)
        unless preds_in_body.include? pred.to_s
          # input interface is underspecified if not used in any rule body
          bud.t_underspecified << [pred, true] # true indicates input mode
          puts "Warning: input interface #{pred} not used"
        end
      else
        # an interface pred is a sink if it is not an input and it is not in any rule's body
        #(if it is in the body, then it is an intermediate node feeding some lhs)
        #bud.sinks << [pred] unless (preds_in_body.include? pred)
        unless preds_in_lhs.include? pred.to_s
          # output interface underspecified if not in any rule's lhs
          bud.t_underspecified << [pred, false]  #false indicates output mode.
          puts "Warning: output interface #{pred} not used"
        end
      end
    end
  end

  def dump_rewrite(strata)
    fout = File.new("#{@bud_instance.class}_rewritten.txt", "w")
    fout.puts "Declarations:"

    strata.each_with_index do |rules, i|
      fout.print "=================================\n"
      fout.print "Stratum #{i}\n"
      rules.each do |r|
        fout.puts "#{r.bud_obj.class}##{r.bud_obj.object_id} #{r.rule_id}"
        fout.puts "\tsrc:      #{r.src}"
        fout.puts "\torig src: #{r.orig_src}"
      end
    end
    fout.close
  end
end
