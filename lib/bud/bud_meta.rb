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
      rce_rewrite
      nodes, stratum_map, top_stratum = stratify_preds

      # stratum_map = {fully qualified pred => stratum}. Copy stratum_map data
      # into t_stratum format.
      raise unless @bud_instance.t_stratum.to_a.empty?
      @bud_instance.t_stratum.merge(stratum_map.to_a)

      # slot each rule into the stratum corresponding to its lhs pred (from stratum_map)
      stratified_rules = Array.new(top_stratum + 2) { [] }  # stratum -> [ rules ]
      @bud_instance.t_rules.each do |rule|
        if rule.op == '<='
          # Deductive rules are assigned to strata based on the basic Datalog
          # stratification algorithm
          belongs_in = stratum_map[rule.lhs]
          # If the rule body doesn't reference any collections, it won't be
          # assigned a stratum, so just place it in stratum zero
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


  Node = Struct.new :name, :status, :stratum, :edges, :in_lhs, :in_body, :in_cycle, :is_neg_head
  # Node.status is one of :init, :in_progress, :done
  Edge = Struct.new :to, :op, :neg, :temporal

  def stratify_preds
    bud = @bud_instance.toplevel
    nodes = {}
    bud.t_depends.each do |d|
      #t_depends [:bud_instance, :rule_id, :lhs, :op, :body] => [:nm, :in_body]
      lhs = (nodes[d.lhs] ||= Node.new(d.lhs, :init, 0, [], true, false, false, false))
      lhs.in_lhs = true
      body = (nodes[d.body] ||= Node.new(d.body, :init, 0, [], false, true, false, false))
      temporal = d.op != "<="
      lhs.edges << Edge.new(body, d.op, d.nm, temporal)
      body.in_body = true
    end

    nodes.each_value {|n| calc_stratum(n, false, false, [n.name])}
    # Normalize stratum numbers because they may not be 0-based or consecutive
    remap = {}
    # if the nodes stratum numbers are [2, 3, 2, 4], remap = {2 => 0, 3 => 1, 4 => 2}
    nodes.values.map {|n| n.stratum}.uniq.sort.each_with_index do |num, i|
      remap[num] = i
    end
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
        next unless edge.op == "<="
        body_stratum = calc_stratum(edge.to, (neg or edge.neg), (edge.temporal or temporal), path + [edge.to.name])
        node.is_neg_head = false # reset for next edge
        node.stratum = max(node.stratum, body_stratum + (edge.neg ? 1 : 0))
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

  # Rewrite the program to apply the Redundant Communication Elimination (RCE)
  # optimization. We consider each channel in turn; if all the downstream
  # consumers (i.e., receivers) of a channel are persistent, then we can avoid
  # repeated deliveries of the same tuple without changing the semantics of the
  # program. To apply RCE to a channel, we (a) create a sender-side
  # "approximation" of the set of delivered channel tuples (b) we add a negation
  # clause to all the rules that derive tuples into the channel -- we only
  # attempt to send messages that aren't in the approximation (c) we insert a
  # protocol to propagate information from receivers to senders in order to
  # fill-in the approximation. Right now, (c) uses a simple unicast ACK'ing
  # protocol, but many variations are possible.
  def rce_rewrite
    # For every channel, consider all rules where the channel appears on either
    # the LHS or the RHS.
    bud = @bud_instance.toplevel
    lhs_ref_chn = Set.new
    rhs_ref_chn = Set.new
    unsafe_chn = Set.new

    # Find all the channels referenced by at least one async rule on the LHS
    bud.t_depends.each do |d|
      if d.op == "<~" and bud.channels.has_key?(d.lhs.to_sym)
        chn_coll = bud.channels[d.lhs.to_sym]
        # bud.channels includes both channels and terminals
        next if chn_coll.nil? or not chn_coll.kind_of? Bud::BudChannel
        lhs_ref_chn.add(d.lhs)
      end
    end

    # Find all channels such that (1) the channel appears on the RHS of at least
    # one deductive rule (2) for every rule where the channel appears on the
    # RHS, the rule is a deductive rule with an LHS collection that is a
    # persistent table or a lattice. The second test is overly conservative but
    # safe; it should be relaxed shortly. (We should allow <+ and maybe <~
    # rules, as well as chains of rules that derive into scratch collections as
    # long as ALL such chains terminate in persistent storage, provided some
    # conditions (e.g., guarded async) are satisfied).
    bud.t_depends.each do |d|
      next unless lhs_ref_chn.include? d.body

      if d.op != "<="
        unsafe_chn.add(d.body)
        next
      end

      lhs_tbl = bud.tables[d.lhs.to_sym]
      unless lhs_tbl and lhs_tbl.kind_of? Bud::BudTable
        unsafe_chn.add(d.body)
        next
      end

      # TODO: check that the rule that derives from chn into the lhs is safe
      # (e.g., joins against a scratch are not allowed).
      rhs_ref_chn.add(d.body)
    end

    rce_chn = rhs_ref_chn - unsafe_chn
    rce_chn.each {|c| rce_for_channel(c)}
  end

  # Apply the RCE optimization to the given channel. That requires two separate
  # things: (1) adding notin clauses to every rule that has the channel on the
  # lhs, to avoid duplicate derivations (2) adding a channel and a communication
  # rule to communicate the set of delivered messages at each remote node. There
  # are multiple strategies possible for #2; right now we just use a simple ACK
  # scheme (one ACK per delivered message).
  #
  # Note that we've done rewriting but not stratification at this point. Hence
  # we need to install dependencies for newly created rules manually.
  def rce_for_channel(chn)
    chn_coll = @bud_instance.channels[chn.to_sym]
    puts "RCE channel: #{chn}"

    # Create an "approx" collection to hold a conservative estimate of the
    # channel tuples that have been delivered.
    approx_name = "#{chn}_approx"
    chn_schema = chn_coll.schema
    puts "DDL: table #{approx_name}, #{chn_schema}"
    @bud_instance.table(approx_name.to_sym, chn_schema)

    ack_name = "#{chn}_ack"
    ack_keys = [:@sender] + chn_coll.key_cols
    ack_schema = { ack_keys => chn_coll.val_cols }
    puts "DDL: channel #{ack_name}, #{ack_schema}"
    @bud_instance.channel(ack_name.to_sym, ack_schema)

    # Install two rules: one to send an ack whenever a channel message is
    # delivered, and another to persist acks in the approx collection.
    install_rule(ack_name, "<~", chn,
                 "#{ack_name} <~ #{chn} {|c| [c.source_address] + c}")
    install_rule(approx_name, "<=", ack_name,
                 "#{approx_name} <= (#{ack_name}.payloads)")

    # Finally, rewrite (delete + recreate) every rule with channel on LHS to add
    # negation against approx collection.
    @bud_instance.t_rules.each do |r|
      add_rce_negation(r, approx_name) if r.lhs == chn
    end
  end

  def add_rce_negation(rule, approx_name)
    # Modify t_rules tuple in-place to change its definition
    rule.src = append_notin(rule.src, approx_name)
    rule.orig_src = append_notin(rule.orig_src, approx_name)

    # Add NM dependency between lhs and approx collection
    depends_tup = [@bud_instance, rule.rule_id, rule.lhs, rule.op,
                   approx_name, true, false]
    @bud_instance.t_depends << depends_tup
  end

  # Add a notin(approx_name) clause to the end of the given Bloom rule. Because
  # of how the <~ operator is parsed (as a superator), we can't easily do this
  # via text munging, so parse into an AST, munge AST, and then get source back.
  def append_notin(src, approx_name)
    parser = RubyParser.for_current_ruby rescue RubyParser.new
    ast = parser.parse(src)

    # Expected format: a top-level call to the "<" method with lhs collection as
    # the receiver. The operand to the < is a call to the ~ method, with the
    # actual rule RHS as the receiver; hence, we want to insert the notin
    # between the original ~ receiver and the ~.
    c1, lhs, angle_op, rhs = ast
    c2, rhs_body, tilde_op = rhs

    raise Bud::CompileError unless c1 == :call and c2 == :call and
                                   angle_op == :< and tilde_op == :~

    rhs[1] = s(:call, rhs_body, :notin,
               s(:call, nil, approx_name.to_sym))

    return Ruby2Ruby.new.process(ast)
  end

  def install_rule(lhs, op, rhs, src)
    # We assume the rule doesn't invoke unsafe functions, is monotonic, and
    # doesn't reference the rhs inside the rule body itself. We also don't
    # bother to do rewriting on the supplied rule text, or check that it is
    # wellformed.
    rule_tup = [@bud_instance, @rule_idx, lhs, op, src, src, false]
    depends_tup = [@bud_instance, @rule_idx, lhs, op, rhs, false, false]

    @bud_instance.t_rules << rule_tup
    @bud_instance.t_depends << depends_tup
    @rule_idx += 1
  end
end
