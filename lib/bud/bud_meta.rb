require 'bud/rewrite'

class BudMeta #:nodoc: all
  def initialize(bud_i)
    @bud_instance = bud_i
    @declarations = bud_i.methods.select {|m| m =~ /^__bloom__.+$/}.map {|m| m.to_s}
    @rule_idx = 0

    # The results of bud_meta are analyzed further using a helper Bloom
    # instance. See depanalysis().
    @dependency_analysis = nil
  end

  def meta_rewrite
    shred_rules # capture dependencies, rewrite rules

    stratified_rules = []
    if @bud_instance.toplevel == @bud_instance
      nodes = compute_node_graph
      analyze_dependencies(nodes)

      stratum_map = stratify_preds(nodes)
      top_stratum = stratum_map.values.max
      top_stratum ||= -1

      # stratum_map = {fully qualified pred => stratum}. Copy stratum_map data
      # into t_stratum format.
      raise unless @bud_instance.t_stratum.to_a.empty?
      @bud_instance.t_stratum.merge(stratum_map.to_a)

      stratified_rules = Array.new(top_stratum + 2) { [] }  # stratum -> [ rules ]
      @bud_instance.t_rules.each do |rule|
        if rule.op == '<='
          # Deductive rules are assigned to strata based on the basic Datalog
          # stratification algorithm. Note that we don't place all the rules
          # with a given lhs relation in the same strata; rather, we place a
          # rule in the lowest strata we can, as determined by the rule's rhs
          # relations (and whether the rel is used in a non-monotonic context).
          body_rels = find_body_rels(rule)
          body_strata = body_rels.map {|r,is_nm| stratum_map[r] + (is_nm ? 1 : 0) || 0}
          belongs_in = body_strata.max

          # If the rule body doesn't reference any collections, it won't be
          # assigned a stratum, so just place it in stratum zero
          belongs_in ||= 0
          stratified_rules[belongs_in] << rule
        else
          # All temporal rules are placed in the last stratum
          stratified_rules[top_stratum + 1] << rule
        end
      end

      # stratified_rules[0] may be empty if none of the nodes at stratum 0 are
      # on the lhs stratified_rules[top_stratum+1] will be empty if there are no
      # temporal rules.
      stratified_rules.reject! {|r| r.empty?}

      stratified_rules.each_with_index do |strat,strat_num|
        strat.each do |rule|
          @bud_instance.t_rule_stratum << [rule.bud_obj, rule.rule_id, strat_num]
        end
      end

      dump_rewrite(stratified_rules) if @bud_instance.options[:dump_rewrite]
    end
    return stratified_rules
  end

  def find_body_rels(rule)
    @bud_instance.t_depends.map do |d|
      [d.body, d.nm] if d.rule_id == rule.rule_id and d.bud_obj == rule.bud_obj
    end.compact
  end

  def shred_rules
    # After making this pass, we no longer care about the names of methods.  We
    # are shredding down to the granularity of rule heads.
    rulebag = {}
    @bud_instance.class.ancestors.reverse.each do |anc|
      @declarations.each do |meth_name|
        rw = rewrite_rule_block(anc, meth_name)
        if rw
          rulebag[meth_name] = rw
          @rule_idx = rw.rule_idx
        end
      end
    end

    rulebag.each_value do |v|
      v.rules.each {|r| @bud_instance.t_rules << r}
      v.depends.each {|d| @bud_instance.t_depends << d}
    end
  end

  def rewrite_rule_block(klass, block_name)
    return unless klass.respond_to? :__bloom_asts__

    pt = klass.__bloom_asts__[block_name]
    return if pt.nil?

    pt = Marshal.load(Marshal.dump(pt)) # deep copy because RuleRewriter mucks up pt
    if @bud_instance.options[:dump_ast]
      require 'pp'
      pp pt
    end
    tmp_expander = TempExpander.new
    pt = tmp_expander.process(pt)
    tmp_expander.tmp_tables.each do |t|
      @bud_instance.temp(t.to_sym)
    end

    rv = check_rule_ast(pt)
    unless rv.nil?
      if rv.class <= Sexp
        error_pt = rv
        error_msg = "parse error"
      else
        error_pt, error_msg = rv
      end

      # try to dump the source code associated with the problematic block, so as
      # to produce a more meaningful error message.
      begin
        code = Ruby2Ruby.new.process(Marshal.load(Marshal.dump(error_pt)))
        src_msg = "\nCode: #{code}"
      rescue Exception
        src_msg = ""
      end
      raise Bud::CompileError, "#{error_msg} in rule block \"#{block_name}\"#{src_msg}"
    end
    rewriter = RuleRewriter.new(@bud_instance, @rule_idx)
    rewriter.process(pt)
    return rewriter
  end

  def get_qual_name(pt)
    # expect to see a parse tree corresponding to a dotted name
    #    a.b.c == s(:call, s1,  :c)
    # where s1 == s(:call, s2,  :b)
    # where s2 == s(:call, nil, :a)
    tag, recv, name, *args = pt
    return nil unless tag == :call and args.empty?

    if recv
      qn = get_qual_name(recv)
      return nil if qn.nil? or qn.size == 0
      return "#{qn}.#{name}"
    else
      return name.to_s
    end
  end

  # Perform some basic sanity checks on the AST of a rule block. We expect a
  # rule block to consist of a :defn whose body consists of a sequence of
  # statements. Each statement is a :call node. Returns nil (no error found), a
  # Sexp (containing an error), or a pair of [Sexp, error message].
  def check_rule_ast(pt)
    # :defn format: node tag, block name, args, body_0, ..., body_n
    tag, name, args, *body = pt
    return pt if tag != :defn

    body.each_with_index do |n,i|
      next if i == 0 and n == s(:nil) # a block got rewritten to an empty block

      # Check for a common case
      if n.sexp_type == :lasgn
        return [n, "illegal operator: '='"]
      end
      return pt unless n.sexp_type == :call and n.length == 4

      # Rule format: call tag, lhs, op, rhs
      _, lhs, op, rhs = n

      # Check that LHS references a named collection
      lhs_name = get_qual_name(lhs)
      return [n, "unexpected lhs format: #{lhs}"] if lhs_name.nil?
      unless @bud_instance.tables.has_key? lhs_name.to_sym or
             @bud_instance.lattices.has_key? lhs_name.to_sym
        return [n, "Collection does not exist: '#{lhs_name}'"]
      end

      return [n, "illegal operator: '#{op}'"] unless [:<, :<=].include? op

      # Check superator invocation. A superator that begins with "<" is parsed
      # as a call to the binary :< operator. The right operand to :< is a :call
      # node; the LHS of the :call is the actual rule body, the :call's oper is
      # the rest of the superator (unary ~, -, +), and the RHS is empty.  Note
      # that ParseTree encodes unary "-" and "+" as :-@ and :-+, respectively.
      # XXX: We don't check for illegal superators (e.g., "<--"). That would be
      # tricky, because they are encoded as a nested unary op in the rule body.
      if op == :<
        return n unless rhs.sexp_type == :call
        op_tail = rhs[2]
        return n unless [:~, :-@, :+@].include? op_tail
        rhs_args = rhs[3..-1]
        return n unless rhs_args.empty?
      end
    end

    return nil # No errors found
  end


  # Node.status is one of :init, :in_progress, :done
  Node = Struct.new :name, :status, :stratum, :edges, :in_lhs, :in_body, :already_neg
  Edge = Struct.new :to, :op, :neg, :temporal

  def compute_node_graph
    nodes = {}
    @bud_instance.toplevel.t_depends.each do |d|
      # t_depends [:bud_instance, :rule_id, :lhs, :op, :body] => [:nm, :in_body]
      lhs = (nodes[d.lhs] ||= Node.new(d.lhs, :init, 0, [], true, false))
      lhs.in_lhs = true
      body = (nodes[d.body] ||= Node.new(d.body, :init, 0, [], false, true))
      body.in_body = true
      temporal = (d.op != "<=")
      lhs.edges << Edge.new(body, d.op, d.nm, temporal)
    end

    return nodes
  end

  def stratify_preds(nodes)
    nodes.each_value {|n| calc_stratum(n, false, false, [n.name])}

    # Normalize stratum numbers because they may not be 0-based or consecutive
    remap = {}
    # if the nodes stratum numbers are [2, 3, 2, 4], remap = {2 => 0, 3 => 1, 4 => 2}
    nodes.values.map {|n| n.stratum}.uniq.sort.each_with_index do |num, i|
      remap[num] = i
    end

    stratum_map = {}
    nodes.each_pair do |name, n|
      n.stratum = remap[n.stratum]
      stratum_map[n.name] = n.stratum
    end
    return stratum_map
  end

  def calc_stratum(node, neg, temporal, path)
    if node.status == :in_process
      if neg and not temporal and not node.already_neg
        raise Bud::CompileError, "unstratifiable program: #{path.uniq.join(',')}"
      end
    elsif node.status == :init
      node.status = :in_process
      node.edges.each do |edge|
        next unless edge.op == "<="
        node.already_neg = neg
        body_stratum = calc_stratum(edge.to, (neg or edge.neg), (edge.temporal or temporal), path + [edge.to.name])
        node.stratum = [node.stratum, body_stratum + (edge.neg ? 1 : 0)].max
      end
      node.status = :done
    end
    node.stratum
  end

  def analyze_dependencies(nodes)  # nodes = {node name => node}
    preds_in_lhs = nodes.select {|_, node| node.in_lhs}.map {|name, _| name}.to_set
    preds_in_body = nodes.select {|_, node| node.in_body}.map {|name, _| name}.to_set

    bud = @bud_instance
    out = bud.options[:stdout]
    out ||= $stdout
    bud.t_provides.each do |p|
      pred, input = p.interface, p.input
      if input
        unless preds_in_body.include? pred
          # input interface is underspecified if not used in any rule body
          bud.t_underspecified << [pred, true] # true indicates input mode
          out.puts "Warning: input interface #{pred} not used"
        end
      else
        unless preds_in_lhs.include? pred
          # output interface underspecified if not in any rule's lhs
          bud.t_underspecified << [pred, false]  # false indicates output mode
          out.puts "Warning: output interface #{pred} not used"
        end
      end
    end
  end

  def depanalysis
    if @dependency_analysis.nil?
      require 'bud/depanalysis'
      da = ::DepAnalysis.new
      da.providing <+ @bud_instance.tables[:t_provides].to_a
      da.depends <+ @bud_instance.t_depends.map{|d| [d.lhs, d.op, d.body, d.nm]}
      da.tick_internal
      @dependency_analysis = da
    end
    @dependency_analysis
  end

  def dump_rewrite(strata)
    fout = File.new("#{@bud_instance.class}_rewritten.txt", "w")
    fout.puts "Declarations:"

    strata.each_with_index do |rules, i|
      fout.puts "================================="
      fout.puts "Stratum #{i}"
      rules.each do |r|
        fout.puts "#{r.bud_obj.class}##{r.bud_obj.object_id} #{r.rule_id}"
        fout.puts "\tsrc:      #{r.src}"
        fout.puts "\torig src: #{r.orig_src}"
      end
    end
    fout.close
  end
end
