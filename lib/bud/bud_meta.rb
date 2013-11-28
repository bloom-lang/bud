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
      rse_rewrite

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
      dump_program if @bud_instance.options[:dump_program]

      if @bud_instance.options[:print_rules]
        rule_ary = @bud_instance.t_rules.map{|r| [r.rule_id, r.orig_src]}.sort
        puts rule_ary.map {|r| "#{r[0]}:\t#{r[1]}"}
      end
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


  # Node.status is one of :init, :in_progress, :done
  Node = Struct.new :name, :status, :stratum, :edges, :in_lhs, :in_body, :already_neg
  Edge = Struct.new :to, :op, :neg, :temporal

  def stratify_preds
    bud = @bud_instance.toplevel
    nodes = {}
    bud.t_depends.each do |d|
      #t_depends [:bud, :rule_id, :lhs, :op, :body] => [:nm, :in_body, :notin_pos_ref, :notin_neg_ref]
      lhs = (nodes[d.lhs] ||= Node.new(d.lhs, :init, 0, [], true, false))
      lhs.in_lhs = true
      body = (nodes[d.body] ||= Node.new(d.body, :init, 0, [], false, true))
      body.in_body = true
      temporal = (d.op != "<=")
      lhs.edges << Edge.new(body, d.op, d.nm, temporal)
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
      if neg and not temporal and not node.already_neg
        raise Bud::CompileError, "unstratifiable program: #{path.uniq.join(',')}"
      end
    elsif node.status == :init
      node.status = :in_process
      node.edges.each do |edge|
        next unless edge.op == "<="
        node.already_neg = neg
        body_stratum = calc_stratum(edge.to, (neg or edge.neg), (edge.temporal or temporal), path + [edge.to.name])
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

  def dump_program
    File.open("#{@bud_instance.class}_rewrite_src.rb", "w") do |f|
      f.puts "class #{@bud_instance.class}_Rewrite"
      f.puts "  include Bud"
      f.puts
      f.puts "  state do"
      @bud_instance.tables.keys.sort {|k1,k2| k1.to_s <=> k2.to_s}.each do |k|
        next if @bud_instance.builtin_tables.has_key? k
        f.puts "    #{coll_ddl_to_str(@bud_instance.tables[k])}"
      end
      f.puts "  end"
      f.puts
      f.puts "  bloom do"
      rule_ary = @bud_instance.t_rules.map{|r| r.orig_src}.sort
      rule_ary.each do |r|
        f.puts "    #{r}"
      end
      f.puts "  end"
      f.puts "end"
    end
  end

  def coll_ddl_to_str(t)
    "#{coll_type_str(t)} :#{t.tabname}, #{coll_schema_str(t)}"
  end

  def coll_type_str(t)
    case t
    when Bud::BudRangeCompress
      "range"
    when Bud::BudSealed
      "sealed"
    when Bud::BudTable
      "table"
    when Bud::BudChannel
      "channel"
    when Bud::BudScratch
      "scratch"
    else
      raise Bud::Error, "unsupported: #{t.class}"
    end
  end

  def coll_schema_str(t)
    schema = t.schema
    if schema.kind_of? Array
      schema
    else
      raise unless schema.size == 1
      "#{schema.keys.first} => #{schema.values.first}"
    end
  end

  # Rewrite the program to apply the Redundant Communication Elimination (RCE)
  # optimization. We consider each channel in turn; if all the downstream
  # consumers (i.e., receivers) of a channel are idempotent, then we can avoid
  # repeated deliveries of the same tuple without changing the semantics of the
  # program. To apply RCE to a channel, we (a) create a sender-side
  # "approximation" of the set of delivered channel tuples (b) we add a negation
  # clause to all the rules that derive tuples into the channel -- we only
  # attempt to send messages that aren't in the approximation (c) we insert a
  # protocol to propagate information from receivers to senders in order to
  # fill-in the approximation. Right now, (c) uses a simple unicast ACK'ing
  # protocol, but many variations are possible.
  def rce_rewrite
    bud = @bud_instance
    return if bud.options[:disable_rce]
    lhs_ref_chn = Set.new
    rhs_ref_chn = Set.new
    unsafe_chn = Set.new

    # Find all the channels that appear on the LHS of at least one async rule
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
    # RHS, the rule is a deductive or @next rule with an LHS collection that is
    # a persistent table or a lattice. The second test is overly conservative
    # but safe; it should be relaxed. (We should allow chains of rules that
    # derive into scratch collections as long as ALL such chains terminate in
    # persistent storage, provided some conditions (e.g., guarded async) are
    # satisfied).
    bud.t_depends.each do |d|
      next unless lhs_ref_chn.include? d.body

      if d.op == "<-"
        unsafe_chn.add(d.body)
        next
      end

      # Note that we allow the receiver to redirect channel output to terminals
      # (stdio). Technically this isn't safe, but it is useful for development.
      lhs_tbl = bud.tables[d.lhs.to_sym]
      unless lhs_tbl and (lhs_tbl.kind_of? Bud::BudTable or lhs_tbl.kind_of? Bud::BudTerminal)
        unsafe_chn.add(d.body)
        next
      end

      # TODO: check that the rule that derives from chn into the lhs is safe
      # (e.g., joins against a scratch are not allowed).
      rhs_ref_chn.add(d.body)
    end

    # XXX: for now, we don't allow any rules where the RHS references a
    # lattice. This should be possible for requires improving the lattice code.
    bud.t_depends.each do |d|
      if d.op == "<~" and lhs_ref_chn.include?(d.lhs) and
         bud.lattices.has_key?(d.body.to_sym)
        unsafe_chn.add(d.lhs)
      end
    end

    rce_chn = rhs_ref_chn - unsafe_chn
    rce_chn.each {|c| rce_for_channel(c)}
  end

  # Apply the RCE optimization to the given channel. That requires two separate
  # things: (1) adding a notin clause to every rule that has the channel on the
  # lhs, to avoid redundant message sends (2) adding a channel and an async rule
  # to communicate the set of delivered messages at each remote node. There are
  # multiple strategies possible for #2; right now we just use a simple ACK
  # scheme (one ACK per delivered message).
  #
  # As an optimization, the ACK message only includes the key columns of the
  # channel; because the keys functionally determine the rest of the message (at
  # the sender-side), sending just the key values is sufficient to supress
  # duplicate deliveries.
  #
  # Note that we've done rewriting but not stratification at this point. Hence
  # we need to install dependencies for newly created rules manually.
  def rce_for_channel(chn)
    chn_coll = @bud_instance.channels[chn.to_sym]
    chn_prefix = chn.gsub(/\./, "__")
    puts "RCE channel: #{chn}" unless @bud_instance.options[:quiet]

    # Create an "approx" collection to hold a conservative estimate of the
    # channel tuples that have been delivered.
    approx_name = "#{chn_prefix}_approx"
    approx_schema = chn_coll.key_cols
    @bud_instance.range(approx_name.to_sym, approx_schema)

    ack_name = "#{chn_prefix}_ack"
    ack_schema = [:@rce_sender] + chn_coll.key_cols
    c = @bud_instance.buf_channel(ack_name.to_sym, ack_schema, true)

    # Install two rules: one to send an ack whenever a channel message is
    # delivered, and another to persist acks in the approx collection.
    key_ary = chn_coll.key_cols.map {|k| "c.#{k}"}
    install_rule(ack_name, "<~", [chn], [],
                 "#{ack_name} <~ #{chn} {|c| [c.source_addr, #{key_ary.join(", ")}]}", false)
    install_rule(approx_name, "<=", [ack_name], [],
                 "#{approx_name} <= (#{ack_name}.payloads)", false)

    # Finally, rewrite (delete + recreate) every rule with channel on LHS to add
    # negation against approx collection.
    @bud_instance.t_rules.each do |r|
      append_rule_negation(r, approx_name, approx_schema) if r.lhs == chn
    end
  end

  def append_rule_negation(rule, rel_name, schema)
    # Modify t_rules tuple in-place to change its definition
    rule.src = append_notin(rule.src, rule.op, rel_name, schema)
    rule.orig_src = append_notin(rule.orig_src, rule.op, rel_name, schema)

    # Add NM dependency between lhs and approx collection
    # XXX: we should also update the dependencies between rule lhs and the left
    # ("positive") input to the notin, to mark notin_pos_ref=true.
    depends_tup = [rule.bud_obj, rule.rule_id, rule.lhs, rule.op,
                   rel_name, true, false, false]
    @bud_instance.t_depends << depends_tup

    # This is a bit gross: if the rule is defined inside an imported module (and
    # hence rule.bud_obj is not the toplevel instance), we need to ensure that
    # approx collection referenced by the negation is defined inside the
    # subordinate Bud instance. Normally definitions would be made first inside
    # the subordinate instance and then automatically copied up to the toplevel,
    # but that import process has already happened by this point.
    if rule.bud_obj != rule.bud_obj.toplevel
      rule.bud_obj.table(rel_name, schema)
    end
  end

  # Add a notin(rel_name, ...) clause to the end of the given Bloom rule; the
  # notin quals check for matches on the collection's key columns.
  #
  # Note that we need to support two slightly different AST formats depending on
  # the rule's operator (<~ is parsed as a superator).
  def append_notin(rule_src, op, rel_name, schema)
    # Generate notin quals for every schema column. Because the notin's left
    # input might not have a well-defined schema (e.g., because it is a join),
    # we use positional notation.
    quals = s(:hash)
    schema.each_with_index do |c,i|
      quals << s(:lit, i)
      quals << s(:lit, c)
    end

    parser = RubyParser.for_current_ruby rescue RubyParser.new
    ast = parser.parse(rule_src)

    if op == "<~" or op == "<+"
      # Expected format: a top-level call to the "<" method with lhs collection
      # as the receiver. The operand to the < is a call to the ~ or + method,
      # with the actual rule RHS as the receiver; hence, we want to insert the
      # notin between the original ~ receiver and the ~/+.
      c1, lhs, angle_op, rhs = ast
      c2, rhs_body, rest_op = rhs

      raise Bud::CompileError unless c1 == :call and c2 == :call and
                                     angle_op == :<

      rhs[1] = s(:call, rhs_body, :notin,
                 s(:call, nil, rel_name.to_sym), quals)
    else
      c1, lhs, ast_op, rhs = ast
      unless c1 == :call and ast_op.to_s == op
        require 'pp'
        pp ast
        puts "ast_op: #{ast_op}; op = #{op}"
      end
      raise Bud::CompileError unless c1 == :call and ast_op.to_s == op

      ast[3] = s(:call, rhs, :notin,
                 s(:call, nil, rel_name.to_sym), quals)
    end

    return Ruby2Ruby.new.process(ast)
  end

  # We assume the rule doesn't invoke unsafe functions and doesn't reference
  # the rhs inside the rule body itself. We also don't bother to do rewriting
  # on the supplied rule text, or check that it is well-formed.
  def install_rule(lhs, op, rhs_rels, rhs_nm_rels, src, is_rse)
    rule_tup = [@bud_instance, @rule_idx, lhs.to_s, op, src, src,
                false, is_rse]
    @bud_instance.t_rules << rule_tup

    [rhs_rels, rhs_nm_rels].each do |ary|
      is_nm = (ary == rhs_nm_rels)
      ary.each do |r|
        depends_tup = [@bud_instance, @rule_idx, lhs.to_s, op, r.to_s,
                       is_nm, false, false]
        @bud_instance.t_depends << depends_tup
      end
    end
    @rule_idx += 1
  end

  # Redundant Storage Elimination (RSE). Look for situations like X.notin(Y),
  # where both X and Y are persistent collections, X does not appear on the RHS
  # of any other rules, and Y does not appear on the LHS of a deletion rule.
  # Hence, when a new tuple appears in Y, the matching tuples in X can be
  # reclaimed (=> physically deleted). We extend this analysis to handle several
  # additional cases that are important in practice:
  #
  #    (1) We allow X = (A*B), where A and B are both persistent collections. In
  #        order to reclaim tuples from A, we need a seal on B (that matches the
  #        join predicate); vice versa for reclaiming tuples from B.
  #
  #    (2) We allow X to appear on the RHS of other rules, provided it does so
  #        in a safe way. For example, Z <= X does not prevent RSE for X tuples,
  #        provided Z is persistent (i.e., either it is a table that is not the
  #        LHS of any delete rules, or it is a scratch and all downstream paths
  #        are persistent). X cannot be referenced in a non-monotonic context
  #        (e.g., Z <= X.group(...), or Z <= T.notin(X)). When X is referenced
  #        in _another_ context in which RSE is applicable (e.g., Z <=
  #        X.notin(T)), we only want to reclaim X when the _intersection_ of the
  #        two RSE conditions is satisfied.
  #
  #    (3) We also want to be able to reclaim from Y, in some situations. When
  #        possible, we want to represent Y using range compression -- when this
  #        is effective, we're fine. But what if Y isn't amenable to range
  #        compression, perhaps because it is sparse or doesn't have a natural
  #        ordering? Let ":a => :b, :c => :d" be the notin qual. If the key
  #        columns of X are [:a, :c], then we know that *at most one* X tuple
  #        will match a given Y tuple. Hence, when we see an X-Y match, we can
  #        discard both X and Y tuples, as long as we can guarantee that no
  #        duplicates of the X tuple are delivered in the future. To do this, we
  #        record all the X key values we've ever seen in a range collection,
  #        X_all_keys; new values are only derived into X if they don't already
  #        appear in X_all_keys.
  #
  # RSE reclaimation is implemented by installing a set of rules (and associated
  # scratch collections) that identify and remove redundant tuples.
  #
  # RSE for joins requires the user to provide seals to identify when partitions
  # of the input are complete. At present, we do this by creating "seal tables"
  # on an as-needed basis (based on the predicates of the join whose inputs we
  # want to reclaim from); the user provides seals by inserting into those
  # tables. This is a bit weird, because the user is inserting into tables that
  # they haven't explicitly created.
  #
  # To handle dependent RSE conditions (see #2 above), we track the dependencies
  # between the individual RSE conditions, and then install a rule that does the
  # deletion when all the conditions are satisfied for a given tuple.
  #
  # NB: we actually don't need to consider the LHS collection of the rule that
  # we apply RSE to -- i.e., given Z <= X.notin(Y), it doesn't matter whether Z
  # is persistent/is deleted from/etc.
  #
  # TODO:
  #   * allow X to be a persistent scratch (defined via monotone rules)
  #   * support projection/selection for X, in addition to join
  #   * refactor: rewrite joins to materialize as a temp rel
  #   * error checking -- right now, we skip notin clauses that are too
  #     complicated for us to parse; however, such a notin might result in
  #     needing to disable/skip RSE. This should be refactored.
  #   * propagate RSE _forward_. That is, given:
  #
  #         foo <= bar.notin(baz)
  #         qux <= (foo * xyz)
  #
  #     A tuples appear in baz, we can clearly delete from bar; morever, we can
  #     also delete from xyz (provided the necessary sealing condition has been
  #     satisfied).
  #
  # LOW PRIORITY:
  #   * support code blocks for notin
  #   * support more tlist expressions for join RSE
  #   * support more join types (e.g., lefts/rights/matches, > 2 way joins, outer)
  #   * check that it works with modules
  #   * efficiency: don't create redundant rules to compute RSE conditions for
  #     collections that we subsequently determine are unsafe
  #   * efficiency: if a relation only has a single RSE condition, we can avoid
  #     creating an extra rule
  def rse_rewrite
    bud = @bud_instance
    return if bud.options[:disable_rse]

    parser = RubyParser.for_current_ruby rescue RubyParser.new
    simple_nots = Set.new
    join_nots = Set.new
    simple_work = Set.new       # simple negations to reclaim from
    join_work = Set.new         # pairs of [join_neg, rels] to reclaim from

    # RSE dependencies: to reclaim from X, the RSE clauses of multiple rules may
    # need to be satisfied. Hence, we use one for each of the rules' RSE
    # conditions, and then an additional rule that intersects all the
    # conditions. This map associates each rel we want to reclaim from with a
    # set of deletion buffers; each deletion buffer contains a tuple when the
    # corresponding RSE condition has been satisfied.
    deps = {}

    # JoinInfo for joins we need to seal against to reclaim a given collection
    seal_deps = {}

    # If there is any rule that means we can't reclaim from a relation, we can't
    # reclaim from that relation for other rules either.
    unsafe_rels = Set.new

    # XXX: we reparse all the rules here, which is unfortunate
    bud.t_rules.each do |r|
      ast = parser.parse(r.orig_src)
      rhs = find_rule_rhs(ast)

      n = NotInCollector.new(simple_nots, join_nots, r, bud)
      n.process(rhs)
    end

    # First, check which of the negation clauses are eligible for RSE.
    simple_nots.each do |neg|
      # If the inner operand (X in X.notin(Y)) is unsafe, declare both X and Y
      # unsafe. Otherwise, X might be safe but Y might not be.
      if check_neg_inner(neg, seal_deps)
        simple_work << neg
        unless check_neg_outer(neg, seal_deps)
          unsafe_rels << neg.outer
        end
      else
        unsafe_rels.merge([neg.inner, neg.outer])
      end
    end

    join_nots.each do |neg|
      do_rels, skip_rels = check_join_not(neg, seal_deps)
      unsafe_rels.merge(skip_rels)
      join_work << [neg, do_rels] unless do_rels.empty?
    end

    # Second, install rules (and transient state) to compute the RSE conditions
    simple_work.each do |neg|
      unless skip_reclaim(neg.inner, unsafe_rels)
        del_tbl_name = create_del_table(neg.inner, neg.rule_id, deps)
        create_del_rule(del_tbl_name, neg.quals, [], neg.inner, neg.outer)
      end

      # Consider whether we should try to reclaim from the outer notin
      # collection ("Y" above).
      do_outer_reclaim(neg, deps) unless skip_reclaim(neg.outer, unsafe_rels)
    end

    join_work.each do |neg, work_rels|
      do_rels = work_rels.reject {|r| skip_reclaim(r, unsafe_rels)}
      next if do_rels.empty?

      # Given a negation that is applied to the output of a join, we first check
      # whether the negation quals only reference fields derived from one or the
      # other of the join inputs. If so, we can reclaim against that join input
      # similar to how we reclaim from SimpleNot -- that is, semantically the
      # notin can be pushed up above the join. However, when the negation quals
      # include columns derived from both join inputs, we need to:
      #
      #    * Create a scratch collection to hold join output tuples that have
      #      appeared in the negation.
      #
      #    * Create a scratch collection to identify join outputs that have not
      #      yet appeared in the negation
      #
      #    * For each collection c we can reclaim from, install rules to reclaim
      #      tuples when (a) there is a seal on c' that matches the join key,
      #      and (b) all the tuples in a given seal group have appeared in the
      #      negation.
      not_qual_rels = find_not_qual_rels(neg)
      missing_buf = nil

      do_rels.each do |r|
        del_tbl_name = create_del_table(r, neg.rule_id, deps)

        if not_qual_rels == [r].to_set
          # Negation qual only references one of the join inputs, so we can
          # install a simpler deletion condition. Unlike with SimpleNot, we need
          # to account for the join's targetlist.
          join_quals, body_quals = quals_tlist_pullup(neg)
          create_del_rule(del_tbl_name, join_quals, body_quals, r, neg.outer)
        else
          if missing_buf.nil?
            join_buf = create_join_buf(neg)
            missing_buf = create_missing_buf(neg, join_buf)
          end
          create_join_del_rules(neg, r, missing_buf, del_tbl_name)
        end
      end
    end

    # Third, install a rule to identify reclamation candidates when a tuple
    # satisfies _all_ of the appropriate RSE conditions, as long as the table
    # doesn't appear in an unsafe context.
    rse_tables = []
    deps.each_pair do |lhs,v|
      next if skip_reclaim(lhs, unsafe_rels)

      rse_table = create_rse_cond_table(lhs)
      rse_tables << [lhs, rse_table]
      puts "RSE: #{lhs}" unless @bud_instance.options[:quiet]
      rule_text = "#{rse_table} <= "
      if v.length == 1
        rule_text << "#{v.first}"
      else
        rule_text << "(#{v.to_a.sort.join(' * ')}).matches "
        block_args = []
        v.to_a.sort.each_with_index {|a,i| block_args << "t#{i}"}
        rule_text << "{|#{block_args.join(',')}| t0}"
      end

      install_rule(rse_table, "<=", v.to_a.sort, [], rule_text, true)
    end

    # Fourth, if the tuple we want to reclaim appears in a join, we need to wait
    # for a compatible seal to ensure the tuple can safely be reclaimed. We can
    # either use a whole-relation seal or a seal that matches a join predicate.
    # Since this is the last condition we need to check, we can also actually do
    # the deletion when this rule is satisfied.
    rse_tables.each do |r|
      reclaim_rel, input_tbl = r

      if seal_deps[reclaim_rel]
        seal_deps[reclaim_rel].each do |seal_dep|
          raise unless reclaim_rel == seal_dep.rse_input

          # First, check whether we need to wait for a seal at all. Given (X*Y)
          # where we want to reclaim from X, suppose the join's targetlist
          # doesn't reference Y (i.e., the join is a semijoin). Hence, once
          # there is a single matching Y tuple, the arrival of subsequent Y
          # tuples will not produce new distinct results. Hence, in this
          # situation we don't need to wait for a seal; rather, we can just wait
          # for a single matching Y tuple, and then allow the corresponding X
          # tuple to be reclaimed.
          if join_is_semijoin(seal_dep)
            output_tbl = install_semijoin_dependency(seal_dep, input_tbl)
          else
            output_tbl = install_join_dependency(seal_dep, input_tbl)
          end

          # Only need to check the next seal dependency once this seal
          # dependency is satisfied
          input_tbl = output_tbl
        end
      end

      # Finally, install the deletion rule
      rule_text = "#{reclaim_rel} <- #{input_tbl}"
      install_rule(reclaim_rel, "<-", [], [input_tbl], rule_text, true)
    end
  end

  def join_is_semijoin(dep)
    false
  end

  def install_semijoin_dependency(dep, input_tbl)
  end

  def install_join_dependency(dep, input_tbl)
    seal_done_tbl = create_seal_done_table(dep.rse_input, dep.other_input)

    # Check for whole-relation seal
    seal_tbl = create_seal_table(dep.other_input)
    rule_text = "#{seal_done_tbl} <= (#{input_tbl} * #{seal_tbl}).lefts"
    install_rule(seal_done_tbl, "<=", [input_tbl, seal_tbl], [],
                 rule_text, true)

    # Check for partition-local seals
    dep.preds.each do |seal_pred|
      if dep.other_input == dep.left_rel
        seal_key = seal_pred.first
      else
        seal_key = seal_pred.last
      end
      seal_tbl = create_seal_table(dep.other_input, seal_key)
      if dep.other_input == dep.left_rel
        join_txt = "(#{seal_tbl} * #{input_tbl}).rights"
      else
        join_txt = "(#{input_tbl} * #{seal_tbl}).lefts"
      end
      rule_text = "#{seal_done_tbl} <= #{join_txt}(:#{seal_pred.first} => :#{seal_pred.last})"
      install_rule(seal_done_tbl, "<=", [input_tbl, seal_tbl], [], rule_text, true)
    end

    return seal_done_tbl
  end

  def do_outer_reclaim(neg, deps)
    outer_rel = @bud_instance.tables[neg.outer]
    inner_rel = @bud_instance.tables[neg.inner]

    # Setup rules to reclaim from outer_rel and suppress duplicate insertions
    # into inner_rel. Note that we can't prevent client programs from inserting
    # duplicates directly into inner_rel from outside Bud, but we just assume
    # that won't happen.
    #
    #   (1) Create range collection to store key values from inner_rel
    #   (2) Create a rule to derive from inner_rel @next into (1)
    #   (3) Rewrite all rules with inner_rel on LHS to negate against (1)
    #   (4) Delete outer_rel values that match anything in (1) but don't appear
    #       in inner_rel
    #
    # Note that #4 is a bit subtle: we only want to reclaim from outer_rel when
    # ALL the conditions for reclaiming the matching inner_rel tuple have been
    # met. The easiest way to do that is to wait until the inner_rel tuple has
    # actually been deleted. Unfortunately that means the inner_rel and
    # outer_rel deletes happen in different ticks, but that's not too important.
    inner_key_range = create_key_range_rel(inner_rel)
    dup_elim_rewrite(inner_rel, inner_key_range)

    del_tbl_name = create_del_table(neg.outer, neg.rule_id, deps)
    inner_neg_quals = neg.quals.invert
    create_del_rule(del_tbl_name, inner_neg_quals, [], outer_rel.tabname,
                    inner_key_range.tabname, neg.inner)
  end

  def skip_reclaim(name, unsafe_rels)
    # Note that we don't try to reclaim from range collections. This is
    # debatable: if we could reclaim non-contiguous elements of the range, the
    # rest of the range collection could be represented more efficiently.
    # Nevertheless, we haven't found any situations like this; plus range tables
    # don't support <- at the moment, although they could.
    unsafe_rels.include?(name) or is_range_tbl(name)
  end

  def is_range_tbl(name)
    @bud_instance.tables[name].kind_of? Bud::BudRangeCompress
  end

  def get_lhs_cols_from_qual(quals, lhs_rel)
    quals.map do |l,r|
      if l.kind_of? Integer
        lhs_rel.cols[l]
      else
        l
      end
    end.to_set
  end

  def create_key_range_rel(src_rel)
    range_name = "#{src_rel.tabname}_all_keys".to_sym
    unless @bud_instance.tables.has_key? range_name
      @bud_instance.range(range_name, src_rel.key_cols)
      install_key_copy_rule(src_rel, @bud_instance.tables[range_name])
    end
    @bud_instance.tables[range_name]
  end

  def install_key_copy_rule(src_rel, range_rel)
    tlist_cols = src_rel.key_cols.map {|c| "r.#{c}"}
    tlist_txt = tlist_cols.join(", ")
    lhs_name = range_rel.tabname.to_s
    rule_txt = "#{lhs_name} <+ #{src_rel.tabname} \{|r| [#{tlist_txt}]\}"
    install_rule(lhs_name, "<+", [src_rel], [], rule_txt, true)
  end

  def dup_elim_rewrite(rel, key_rel)
    @bud_instance.t_rules.each do |r|
      if r.lhs == rel.tabname.to_s and (r.op == "<=" or r.op == "<+")
        append_rule_negation(r, key_rel.tabname.to_s, key_rel.schema)
      end
    end
  end

  # Returns all the join input relations referenced by the negation's list of
  # quals. That is, given (x * y).pairs {...}.notin(z, :k1 => :k2, :k3 => :k4),
  # we want to find whether k1 and k3 are derived from x, y, or both.
  def find_not_qual_rels(jneg)
    rel_offset_map = []
    jneg.tlist.each_with_index do |t,i|
      # Skip constant TLEs, because they are derived from neither join input
      if t.kind_of? TListVarRef
        rel_offset_map[i] = t.var_name
      end
    end

    # If no negation qual is given explicitly, the negation qual is implicitly
    # the entire join output tuple (columns matched based on position). Hence,
    # all the rels referenced by the tlist are referenced by the qual list.
    if jneg.not_quals.empty?
      return rel_offset_map.to_set
    else
      rels = Set.new

      jneg.not_quals.each do |q|
        # The rhs of the negation qual references the notin's negative input, so
        # we can ignore it
        lhs, rhs = q

        if lhs.kind_of? Integer
          # If the qual lhs references a constant TLE, skip it
          next if rel_offset_map[lhs].nil?
          rels << rel_offset_map[lhs]
        else
          # XXX: If the qual lhs is a column name, conservatively assume it
          # could be derived from either join input (for now)
          return rel_offset_map.to_set
        end
      end

      return rels
    end
  end

  # The negation quals are expressed with respect to the *output* of the join's
  # targetlist. We want to pull the negation quals up, so that they are
  # expressed with respect to the join input collections. We limit the supported
  # tlist expressions so that this is always possible. We return two lists of
  # quals: quals that reference variables (which can be evaluated as join quals
  # in the deletion rule), and quals that reference constants (which must be
  # evaluated in the body of the deletion rule, because Bud's join syntax
  # doesn't join quals to reference literals).
  def quals_tlist_pullup(jneg)
    # If no explicit negation qual, there is an implicit qual against every
    # field in the join tlist. Since we still need to pullup the qual above the
    # tlist, first transform an implicit qual into an equivalent explicit qual.
    if jneg.not_quals.empty?
      outer_rel = @bud_instance.tables[jneg.outer]
      not_quals = {}
      jneg.tlist.each_with_index do |tle,i|
        not_quals[i] = outer_rel.cols[i]
      end
    else
      not_quals = jneg.not_quals
    end

    join_quals = {}
    body_quals = []
    not_quals.each do |q|
      lhs, rhs = q      # We only need to pullup the lhs

      raise unless lhs.kind_of? Integer
      tle = jneg.tlist[lhs]
      if tle.kind_of? TListVarRef
        join_quals[tle.col_name] = rhs
      else
        body_quals << [rhs, const_to_str(tle)]
      end
    end

    return join_quals, body_quals
  end

  # Install a rule that contains populates a collection with an RSE
  # condition. We don't actually do the deletion here -- a subsequent rule does
  # a physical deletion when the conjunction of the RSE conditions for a given
  # tuple has been met.
  def create_del_rule(del_tbl, join_quals, body_quals, r1, r2, not_rel=nil)
    # XXX: we currently don't need to support any of these cases
    raise if join_quals.empty? and not body_quals.empty?
    raise if not_rel and not body_quals.empty?
    raise if not_rel and join_quals.empty?

    if join_quals.empty?
      install_rule(del_tbl, "<=", [r2], [],
                   "#{del_tbl} <= #{r2}", true)
    else
      qual_ary = []
      join_quals.each do |k,v|
        qual_ary << "#{k.inspect} => #{v.inspect}"
      end
      qual_txt = qual_ary.join(", ")

      if body_quals.empty?
        join_clause = "lefts(#{qual_txt})"
      else
        body_qual_text = body_quals_to_str(body_quals, "r")
        join_clause = "pairs(#{qual_txt}) {|l,r| l#{body_qual_text}}"
      end

      rhs = "(#{r1} * #{r2}).#{join_clause}"
      nm_rels = []
      if not_rel
        rhs = "(#{rhs}).notin(#{not_rel}, #{qual_txt})"
        nm_rels << not_rel
      end

      install_rule(del_tbl, "<=", [r1, r2], nm_rels, "#{del_tbl} <= #{rhs}", true)
    end
  end

  def check_join_not(jneg, seal_deps)
    do_rels = Set.new
    skip_rels = jneg.join_rels.to_set
    seal_deps = Hash.new

    # TODO: Notin code blocks not supported
    if jneg.not_block.nil? and rel_is_inflationary(jneg.outer, Set.new)
      jneg.join_rels.each do |r|
        if can_reclaim_rel(r, jneg.rule_id, jneg.bud_obj, Set.new, seal_deps)
          do_rels << r
          skip_rels.delete(r)
        end
      end
    end

    return do_rels, skip_rels
  end

  def create_rse_cond_table(target)
    tbl_name = "rse_ready_#{target}"
    target_tbl = @bud_instance.tables[target]
    @bud_instance.scratch(tbl_name.to_sym, target_tbl.schema)
    return tbl_name
  end

  def create_seal_done_table(input, seal_tbl)
    tbl_name = "seal_done_#{input}_#{seal_tbl}"
    input_tbl = @bud_instance.tables[input]
    @bud_instance.scratch(tbl_name.to_sym, input_tbl.schema)
    return tbl_name
  end

  # For each _rule_, we create a scratch to identify when the RSE condition for
  # that rule has been satisfied. Note that we only create one table per rule;
  # if there are multiple negations chained together into a single rule
  # (x.notin(y).notin(z)), we can reclaim from x when EITHER y or z is
  # satisfied -- so we create a single scratch for the rule.
  def create_del_table(target, rule_id, deps)
    tbl_name = "del_#{target}_r#{rule_id}"
    deps[target] ||= Set.new
    deps[target] << tbl_name

    unless @bud_instance.tables.has_key? tbl_name.to_sym
      target_tbl = @bud_instance.tables[target]
      @bud_instance.scratch(tbl_name.to_sym, target_tbl.schema)
    end

    return tbl_name
  end

  def create_join_buf(jneg)
    # Define the LHS (join buf) collection -- this collection contains all the
    # join input tuples that have a match in the negated collection. The
    # collection's schema is simply the concatenation of the columns from both
    # join inputs; we disambiguate column names by adding a prefix.
    lhs, rhs = jneg.join_rels
    lhs_name = "r#{jneg.rule_id}_#{lhs}_#{rhs}_joinbuf"
    lhs_schema = []
    jneg.join_rels.each do |r|
      r_coll = @bud_instance.tables[r.to_sym]
      r_coll.cols.each do |c|
        lhs_schema << "#{r}_#{c}".to_sym
      end
    end
    @bud_instance.scratch(lhs_name.to_sym, lhs_schema)

    # Build the join predicate. We want the original join predicates. We also
    # want to matchup the negation quals against the elements of the join's
    # targetlist. That is, given
    #
    #    ((foo * bar).pairs {|x,y| [x.a, y.b]}).notin(baz, 1 => :k)
    #
    # We want to match baz.k with the second element of the join tlist (bar.b).
    qual_list = join_quals_to_str_ary(jneg)
    outer_rel = @bud_instance.tables[jneg.outer.to_sym]

    # If the join's targetlist contains a constant expression, we need to check
    # that values found in the negated collection match the constant (you might
    # think we could ignore such constants, but if multiple rules derive tuples
    # into the same negated collection, we need to avoid considering tuples
    # produced by a different rule). Since we can't check constants in the join
    # predicate clause, instead check them in the body of the generated rule.
    body_quals = []

    # If no negation qual is given explicitly, the negation qual is implicitly
    # the entire tuple (columns matched based on position).
    if jneg.not_quals.empty?
      jneg.tlist.each_with_index do |t,i|
        if t.kind_of? TListVarRef
          i_col = outer_rel.cols[i]
          qual_list << "#{jneg.outer}.#{i_col} => #{t.var_name}.#{t.col_name}"
        else
          body_quals << [i, const_to_str(t)]
        end
      end
    else
      jneg.not_quals.each do |q|
        # We expect the left part of the qual (which references the output of
        # the join) to be specified as a column offset.
        lhs_qual_idx, rhs_qual = q
        raise unless lhs_qual_idx.kind_of? Integer

        tle = jneg.tlist[lhs_qual_idx]
        raise if tle.nil?

        # RHS qual can be either column name or offset.
        if rhs_qual.kind_of? Integer
          rhs_qual = outer_rel.cols[rhs_qual]
        end

        if tle.kind_of? TListVarRef
          qual_list << "#{jneg.outer}.#{rhs_qual} => #{tle.var_name}.#{tle.col_name}"
        else
          body_quals << [lhs_qual_idx, const_to_str(tle)]
        end
      end
    end

    if jneg.is_outer_join
      jtype = "outer_combos"
    else
      jtype = "combos"
    end
    body_qual_text = body_quals_to_str(body_quals, "z")
    qual_text = "(" + qual_list.join(", ") + ")"
    rhs_text = "(#{lhs} * #{rhs} * #{jneg.outer}).#{jtype}#{qual_text} {|x,y,z| x + y#{body_qual_text}}"
    rule_text = "#{lhs_name} <= #{rhs_text}"
    install_rule(lhs_name, "<=", jneg.join_rels + [jneg.outer], [], rule_text, true)

    return lhs_name
  end

  def body_quals_to_str(quals, rel_prefix)
    qual_text = ""
    unless quals.empty?
      qual_ary = quals.map do |q|
        field, const = q
        if field.kind_of? Integer
          field_text = "[#{field}]"
        else
          field_text = ".#{field}"
        end
        "#{rel_prefix}#{field_text} == #{const}"
      end
      qual_text << " if "
      qual_text << qual_ary.join(" and ")
    end
    qual_text
  end

  def const_to_str(tl_const)
    Ruby2Ruby.new.process(Marshal.load(Marshal.dump(tl_const.const_expr)))
  end

  def join_quals_to_str_ary(jneg)
    lhs, rhs = jneg.join_rels
    jneg.join_quals.map do |q|
      "#{lhs}.#{q[0]} => #{rhs}.#{q[1]}"
    end
  end

  def create_missing_buf(jneg, join_buf)
    lhs, rhs = jneg.join_rels
    lhs_name = "r#{jneg.rule_id}_#{lhs}_#{rhs}_missing"
    join_buf_rel = @bud_instance.tables[join_buf.to_sym]
    @bud_instance.scratch(lhs_name.to_sym, join_buf_rel.schema)

    qual_list = join_quals_to_str_ary(jneg)
    if qual_list.empty?
      qual_text = ""
    else
      qual_text = "(" + qual_list.join(", ") + ")"
    end

    if jneg.is_outer_join
      jtype = "outer"
    else
      jtype = "pairs"
    end
    rhs_text = "(#{lhs} * #{rhs}).#{jtype}#{qual_text} {|x,y| x + y}.notin(#{join_buf})"
    rule_text = "#{lhs_name} <= #{rhs_text}"
    install_rule(lhs_name, "<=", jneg.join_rels, [join_buf], rule_text, true)

    return lhs_name
  end

  # Install rules to reclaim from "rel" when legal. Reclaiming from "rel"
  # requires looking for seals against the other operand in the join that
  # involves "rel". We can make use of seals on each of the join qualifiers,
  # plus "whole-relation" seals (i.e., seals that guarantee that one of the join
  # input collections cannot grow in the future).
  def create_join_del_rules(jneg, rel, missing_buf, del_tbl_name)
    if rel == jneg.join_rels.first
      other_rel = jneg.join_rels.last
    else
      other_rel = jneg.join_rels.first
    end

    notin_quals = []
    rel_tbl = @bud_instance.tables[rel.to_sym]
    rel_tbl.cols.each do |c|
      notin_quals << ":#{c} => :#{rel}_#{c}"
    end
    qual_str = notin_quals.join(", ")

    jneg.join_quals.each do |q|
      if other_rel == jneg.join_rels.first
        orel_qual, rel_qual = q
      else
        rel_qual, orel_qual = q
      end
      seal_name = create_seal_table(other_rel, orel_qual)
      rhs_text = "(#{rel} * #{seal_name}).lefts(:#{rel_qual} => :#{orel_qual}).notin(#{missing_buf}, #{qual_str})"
      rule_text = "#{del_tbl_name} <= #{rhs_text}"
      install_rule(del_tbl_name, "<=", [rel, seal_name], [missing_buf], rule_text, true)
    end

    # Whole-relation seals; the column in the seal relation is ignored, but we
    # add a dummy column to avoid creating a collection with zero columns.
    seal_name = create_seal_table(other_rel)
    rhs_text = "(#{rel} * #{seal_name}).lefts.notin(#{missing_buf}, #{qual_str})"
    rule_text = "#{del_tbl_name} <= #{rhs_text}"
    install_rule(del_tbl_name, "<=", [rel, seal_name], [missing_buf], rule_text, true)
  end

  def create_seal_table(rel, seal_key=nil)
    if seal_key.nil?         # Whole-relation seal
      seal_name = "seal_#{rel}"
      schema = [:ignored]
    else
      seal_name = "seal_#{rel}_#{seal_key}"
      schema = [seal_key.to_sym]
    end

    unless @bud_instance.tables.has_key? seal_name.to_sym
      @bud_instance.table(seal_name.to_sym, schema)
    end

    seal_name
  end

  def check_neg_inner(neg, seal_deps)
    # Skip notin self joins: RSE would result in inferring a deletion rule for
    # the collection, which would then make RSE illegal.
    return false if neg.inner == neg.outer

    # TODO: Notin code blocks not supported yet
    return false unless neg.block.nil?

    return can_reclaim_rel(neg.inner, neg.rule_id, neg.bud_obj, Set.new, seal_deps) &&
           rel_is_inflationary(neg.outer, Set.new)
  end

  def check_neg_outer(neg, seal_deps)
    return false unless can_reclaim_rel(neg.outer, neg.rule_id,
                                        neg.bud_obj, Set.new, seal_deps)

    outer_rel = @bud_instance.tables[neg.outer]

    # Heuristic: we assume that if outer_rel is a range, range compression will
    # be effective, so we needn't reclaim from it.
    return false if outer_rel.kind_of? Bud::BudRangeCompress

    # For now, don't try to reclaim from outer_rel unless it is directly
    # persisted (in principle, we could reclaim from scratches that are defined
    # purely via monotone rules over persistent collections).
    return false unless outer_rel.kind_of? Bud::BudTable

    # Check whether the negation quals are _exactly_ the keys of the inner
    # relation; otherwise, suppressing duplicates from the inner is not
    # sufficient to allow us to reclaim from the outer.
    inner_rel = @bud_instance.tables[neg.inner]
    inner_keys = inner_rel.key_cols.to_set
    neg_inner_qual_cols = get_lhs_cols_from_qual(neg.quals, inner_rel)
    return neg_inner_qual_cols == inner_keys
  end

  def can_reclaim_rel(rel, rule_id, bud_obj, seen_rels, seal_deps)
    # XXX: generalize this to allow projection/selection/scratches
    return false unless is_persistent_tbl(rel)

    # Consider all the collections whose contents are derived (directly or
    # indirectly) from "rel". For each such dependency, we want to check that
    # reclaiming tuples from rel is "safe". Given X.notin(Y), consider a RHS
    # reference to X:
    #
    # R <= X { ... }
    #
    # This is safe when the rule is monotonic and R is either "safe"
    # (recursively) or directly persisted.
    @bud_instance.t_depends.each do |d|
      next if d.rule_id == rule_id and d.bud_obj == bud_obj
      next if seen_rels.include? d
      if d.body == rel.to_s
        return false unless is_safe_rhs_ref(rel, d, seen_rels, seal_deps)
      end
    end

    return true
  end

  def is_safe_rhs_ref(rel, ref_depend, seen_rels, seal_deps)
    return false if ref_depend.in_body
    return false if (ref_depend.nm and not ref_depend.notin_neg_ref)

    # We want to check if it is safe to reclaim from X, which appears in a join
    # with Y (we assume the join is binary). We can still reclaim from X, but
    # doing so requires proving that no more join result tuples will be produced
    # that depend on X. To consider this, we need to analyze the semantics of
    # the join. Unfortunately, this information is not readily available from
    # the t_depend record, so we need to look up the rule's source, parse it,
    # find the join fragment of the AST, and then analyze that.
    if ref_depend.join_ref
      join_info = lookup_join_info(rel, ref_depend.bud_obj, ref_depend.rule_id)
      return false unless join_info

      # Make reclaiming from X dependent on seeing a matching seal against Y.
      seal_deps[rel] ||= []
      seal_deps[rel] << join_info
    end

    dependee = ref_depend.lhs.to_sym
    return false if is_deleted_tbl(dependee)
    return true if is_persistent_tbl(dependee)
    if ref_depend.notin_pos_ref
      new_seen_rels = seen_rels + [ref_depend]
      return true if can_reclaim_rel(rel, ref_depend.rule_id, ref_depend.bud_obj,
                                     new_seen_rels, seal_deps)
    end
    # XXX: Shouldn't we also check can_reclaim_rel here?
    return true if ref_depend.notin_neg_ref

    # If the LHS of a rule that references "rel" is not persistent, we need to
    # determine whether the LHS is safe.
    saw_ref = false
    @bud_instance.t_depends.each do |d|
      next if d == ref_depend
      if d.body == dependee.to_s
        saw_ref = true
        # XXX: test case for seal_deps here
        return false unless is_safe_rhs_ref(dependee, d, Set.new, seal_deps)
      end
    end

    return saw_ref
  end

  def rel_is_inflationary(rel, done_deps)
    return false if is_deleted_tbl(rel)
    return true if is_persistent_tbl(rel)

    # If the negated input to the notin is a scratch, we need to determine
    # whether the scratch will only grow over time -- that is, all of its input
    # collections must grow over time and the rules that are used to derive the
    # scratch collection must be monotone.
    #
    # XXX: code cleanup / unification with other dependency checks
    saw_ref = false
    @bud_instance.t_depends.each do |d|
      if d.lhs == rel.to_s and not done_deps.include? d
        done_deps << d
        saw_ref = true
        return false if d.nm or d.in_body or d.op == "<~"
        return false unless rel_is_inflationary(d.body.to_sym, done_deps)
      end
    end

    return saw_ref
  end

  # Does "t" appear on the LHS of any deletion rules? Note that we distinguish
  # between deletion rules specified by the user and deletion rules inserted by
  # RSE; the latter are allowed, because the RSE conditions should ensure that
  # RSE deletions are not semantically observable.
  def is_deleted_tbl(t)
    @bud_instance.t_depends.each do |d|
      if d.lhs == t.to_s and d.op == "<-"
        rule = @bud_instance.t_rules[[d.bud_obj, d.rule_id]]
        return true unless rule.is_rse
      end
    end

    return false
  end

  def is_persistent_tbl(t)
    @bud_instance.tables[t].kind_of? Bud::BudTable
  end

  # Given a table involved in a join, return metadata about the join -- the
  # other join input, the join predicates, and the join's targetlist. Note that
  # we're given the rule ID; we assume the table appears in exactly one join in
  # that rule, and that the join is binary.
  def lookup_join_info(tbl, bud_obj, rule_id)
    rule = @bud_instance.t_rules[[bud_obj, rule_id]]
    parser = RubyParser.for_current_ruby rescue RubyParser.new
    ast = parser.parse(rule.orig_src)

    join_parse = JoinInfoParser.new(tbl)
    join_parse.process(ast)
    join_parse.join_info
  end

  def find_rule_rhs(ast)
    raise Bud::Error unless ast.sexp_type == :call

    # We need to distinguish between normal rules and <~ rules -- the latter
    # invoke a superator, which has a different AST
    _, lhs, op, rest = ast
    if op == :<
      # Async rule (superator invocation)
      raise Bud::Error unless rest.sexp_type == :call
      _, rhs, tilde_op = rest
    else
      rhs = rest
    end

    return rhs
  end

  # We support two syntax variants for the join quals: ":foo => :bar" or "x.foo
  # => y.bar". We transform the latter into the former by consulting the
  # optional list of join rels supplied by the caller (we'll need to do
  # something smarter if/when non-binary joins are supported). The return value
  # is a hash of {:a => :b} pairs for each ":a => :b" qual (regardless of the
  # input syntax).
  def self.parse_qual_ast(h, join_rels=nil)
    return {} if h.nil?

    qual_ary = h.sexp_body.each_slice(2).map do |q1, q2|
      types = [q1, q2].map(&:sexp_type)
      if types == [:lit, :lit]
        l = q1.sexp_body.first
        r = q2.sexp_body.first
        {l => r}
      elsif types == [:call, :call]
        l_rel, l_col = parse_call_ref(q1, join_rels)
        r_rel, r_col = parse_call_ref(q2, join_rels)
        if l_rel == join_rels.first
          {l_col => r_col}
        else
          {r_col => l_col}
        end
      else
        raise
      end
    end

    qual_ary.reduce({}) {|h, pair| h.merge(pair)}
  end

  def self.parse_call_ref(c, rels)
    raise if rels.nil?
    _, recv, col_name = c
    raise unless recv.sexp_type == :call
    _, _, rel_name = recv
    raise unless rels.include? rel_name
    return rel_name, col_name
  end

  TListVarRef = Struct.new(:var_name, :col_name)
  TListConst = Struct.new(:const_expr)
  CodeBlock = Struct.new(:args, :body)

  # Search an AST to look for notin operators that are candidates for RSE. We
  # distinguish between two types of notins: "simple" notins (where the notin
  # receiver is a collection) and notins applied to a join expression.
  class NotInCollector < SexpProcessor
    SimpleNot = Struct.new(:inner, :outer, :quals, :block, :rule_id, :bud_obj)
    JoinNot = Struct.new(:join_rels, :join_quals, :is_outer_join, :tlist, :outer,
                         :not_quals, :not_block, :rule_id, :bud_obj)

    def initialize(simple_nots, join_nots, rule, bud)
      super()
      self.require_empty = false
      self.expected = Sexp
      @simple_nots = simple_nots
      @join_nots = join_nots
      @rule = rule
      @bud_instance = bud
    end

    # If the notin has a code block, we see:
    #     s(:iter, s(:call, ..., :notin, ...), s(:args, ...), body)
    # i.e., we'll see the iter first.
    def process_iter(exp)
      _, recv, args, *body = exp

      if recv.sexp_type == :call
        block = CodeBlock.new(args, body)
        process_call(recv, block)
      else
        process(recv)
      end

      exp
    end

    def process_call(exp, code_block=nil)
      _, recv, meth, *args = exp

      if meth == :notin
        collect_notin(exp, code_block)
      else
        process(recv) unless recv.nil?
        args.each {|a| process(a)}
      end

      exp
    end

    def collect_notin(exp, code_block=nil)
      _, recv, meth, *args = exp

      # If the notin receiver is passed a code block, it can't be a simple
      # notin, but it might still be a join notin
      if recv.sexp_type == :iter
        return collect_iter_notin(recv, args)
      end

      # We also need to check for joins that don't have a code block; we assume
      # this only happens for lefts and rights. We implement this by just
      # inserting a no-op code block that projects the entire left/right input
      # tuple, and then process as normal.
      if recv.sexp_type == :call
        _, r_recv, r_meth, *r_args = recv
        if [:lefts, :rights].include? r_meth
          new_recv = s(:iter,
                       recv,
                       s(:args, :x),
                       s(:lvar, :x))

          return collect_iter_notin(new_recv, args)
        end
      end

      # We support two kinds of "simple" negations: the inner operand can either
      # be a collection (referenced directly), or another notin expression.
      outer_info = []
      while true
        if recv.nil? and args.empty?
          inner = meth
          break
        elsif meth == :notin
          outer, quals = collect_notin_args(args)
          outer_info << [outer, quals]

          return unless recv.sexp_type == :call
          _, recv, meth, *args = recv
        else
          return
        end
      end

      outer_info.each do |o|
        outer, quals = o
        @simple_nots << SimpleNot.new(inner, outer, quals, code_block,
                                      @rule.rule_id, @rule.bud_obj)
      end
    end

    def collect_notin_args(args)
      # First argument is the outer operand to the notin. If present, second
      # argument is a hash of notin quals.
      outer, quals = args

      raise unless outer.sexp_type == :call
      _, o_recv, o_meth, o_args = outer
      raise unless o_recv.nil? and o_args.nil?

      qual_h = {}
      if quals
        raise unless quals.sexp_type == :hash
        qual_h = BudMeta.parse_qual_ast(quals)
      end

      return o_meth, qual_h
    end

    def collect_iter_notin(recv, args)
      _, i_recv, i_block_args, i_body = recv

      # First, we need to determine whether this is a join. Right now, we only
      # support binary inner joins.
      return unless i_recv.sexp_type == :call
      _, c_recv, c_meth, c_args = i_recv
      return unless [:pairs, :combos, :lefts, :rights, :outer].include? c_meth

      join_rels = get_join_rels(c_recv)
      return unless join_rels

      # Optional join predicates
      join_quals = []
      if c_args
        return unless c_args.sexp_type == :hash
        join_quals = BudMeta.parse_qual_ast(c_args, join_rels)
      end

      tlist = get_tlist(i_block_args, i_body, join_rels)
      return if tlist.nil?
      outer, not_quals = collect_notin_args(args)
      @join_nots << JoinNot.new(join_rels, join_quals, c_meth == :outer,
                                tlist, outer, not_quals,
                                nil, @rule.rule_id, @rule.bud_obj)
    end

    # Find the targetlist by looking at the body of the iter code block. We only
    # support very simple targetlist expressions: array literals containing
    # simple column references or constants, addition (array concatenation)
    # operators, and whole-tuple references. We resolve column references by
    # looking up the local variable names introduced by the iter block args;
    # whole tuple refs are expanded by looking at the catalog. Note that because
    # we resolve tlist column refs here, we don't need to distinguish between
    # left/rights/pairs joins in subsequent analysis.
    def get_tlist(block_args, block_body, join_rels)
      var_tbl = {}
      var_list = block_args.sexp_body
      var_list.each_with_index {|v,i| var_tbl[v] = join_rels[i]}

      catch (:skip) do
        return get_tlist_from_ast(block_body, var_tbl, join_rels)
      end
    end

    def get_tlist_from_ast(ast, var_tbl, join_rels)
      case ast.sexp_type
      when :array
        ast.sexp_body.map {|e| tlist_array_lit(e, var_tbl)}
      when :call
        _, recv, op, args = ast
        throw :skip unless op == :+
        get_tlist_from_ast(recv, var_tbl, join_rels) + get_tlist_from_ast(args, var_tbl, join_rels)
      when :lvar
        _, ref_var = ast
        throw :skip unless var_tbl.has_key? ref_var
        ref_tbl_name = var_tbl[ref_var]
        ref_coll = @bud_instance.tables[ref_tbl_name]
        throw :skip if ref_coll.nil?
        ref_coll.cols.map {|c| TListVarRef.new(ref_tbl_name, c)}
      else
        throw :skip
      end
    end

    # We expect an array literal to contain a combination of column references
    # (x.y) and constant values; as a special-case, we regard the builtin
    # functions "port" and "ip_port" as constants.
    def tlist_array_lit(ref, var_tbl)
      case ref.sexp_type
      when :call
        _, recv, meth, *args = ref
        if recv.nil? and (meth == :ip_port or meth == :port)
          return TListConst.new(ref)
        end
        throw :skip if recv.nil? or recv.sexp_type != :lvar
        ref_var = recv.sexp_body.first
        throw :skip unless var_tbl.has_key? ref_var
        TListVarRef.new(var_tbl[ref_var], meth)
      when :str, :lit
        TListConst.new(ref)
      else
        throw :skip
      end
    end

    def get_join_rels(join_ast)
      return unless join_ast.sexp_type == :call

      _, left, star_op, right = join_ast
      return unless star_op == :*
      return unless left.sexp_type == :call
      return unless right.sexp_type == :call

      _, l_recv, l_meth = left
      _, r_recv, r_meth = right

      return l_meth, r_meth
    end
  end

  JoinInfo = Struct.new(:rse_input, :other_input, :left_rel, :right_rel,
                        :join_type, :preds, :tlist)

  class JoinInfoParser < SexpProcessor
    def initialize(tbl)
      super()
      self.require_empty = false
      self.expected = Sexp
      @known_input = tbl
    end

    def process_call(exp, code_block=nil)
      _, recv, meth, *args = exp

      is_join, @join_type, @left_rel, @right_rel = parse_join_call(recv, meth)
      if is_join
        if @left_rel == @known_input
          @other_input = @right_rel
        elsif @right_rel = @known_input
          @other_input = @left_rel
        else
          return exp
        end

        @join_preds = BudMeta.parse_qual_ast(args.first)
        @tlist = code_block
      else
        process(recv) unless recv.nil?
        args.each {|a| process(a)}
      end

      exp
    end

    def parse_join_call(recv, meth)
      if [:pairs, :lefts, :rights].include? meth and recv and recv.sexp_type == :call
        _, r_recv, r_meth, *r_args = recv
        if r_meth == :*
          left_rel = call_to_rel_name(r_recv)
          right_rel = call_to_rel_name(r_args.first)
          return true, meth, left_rel, right_rel if left_rel and right_rel
        end
      end

      return false
    end

    def join_info
      if @other_input
        JoinInfo.new(@known_input, @other_input, @left_rel, @right_rel,
                     @join_type, @join_preds, @tlist)
      else
        nil
      end
    end

    def process_iter(exp)
      _, recv, args, *body = exp

      if recv.sexp_type == :call
        block = CodeBlock.new(args, body)
        process_call(recv, block)
      else
        process(recv)
      end

      exp
    end

    def call_to_rel_name(exp)
      _, recv, meth, *args = exp
      if recv.nil? and args.empty?
        meth
      else
        nil
      end
    end
  end
end
