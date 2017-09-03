require 'rubygems'

class RuleRewriter < Ruby2Ruby # :nodoc: all
  attr_accessor :rule_idx, :rules, :depends

  OP_LIST = [:<<, :<, :<=].to_set
  TEMP_OP_LIST = [:-@, :~, :+@].to_set
  MONOTONE_WHITELIST = [:==, :+, :<=, :-, :<, :>, :*, :~, :+@,
                        :pairs, :matches, :combos, :flatten, :new,
                        :lefts, :rights, :map, :flat_map, :pro, :merge,
                        :schema, :cols, :key_cols, :val_cols, :payloads, :lambda,
                        :tabname, :current_value].to_set

  def initialize(bud_instance, rule_idx)
    @bud_instance = bud_instance
    @tables = {}
    @nm = false
    @rule_idx = rule_idx
    @collect = false
    @rules = []
    @depends = []
    @iter_stack = []
    @refs_in_body = Set.new
    super()
  end

  $not_id = [:not_coll_id]
  def resolve(obj, prefix, name)
    qn = prefix ? prefix + "." + name.to_s : name.to_s
    return [:collection, qn, obj.tables[name]] if obj.tables.has_key? name
    return [:lattice, qn, obj.lattices[name]] if obj.lattices.has_key? name

    # does name refer to an import name?
    iobj = obj.import_instance name
    return [:import, qn, iobj] if iobj and iobj.respond_to? :tables

    return $not_id
  end

  def exp_id_type(recv, name, args) # call only if sexp type is :call
    return $not_id unless args.empty?
    ty = $not_id
    if recv
      if recv.first == :call
        # possibly nested reference
        # rty, rqn, .. = receiver's type, qual name etc.
        rty, rqn, robj = exp_id_type(recv[1], recv[2], recv[3..-1])
        ty = resolve(robj, rqn, name) if rty == :import
      end
    else
      # plain, un-prefixed name. See if it refers to a collection or import spec
      ty = resolve(@bud_instance, nil, name)
    end
    ty
  end

  def call_to_id(exp)
    # convert a series of nested calls, a sexp of the form
    #   s(:call,
    #       s(:call, s(:call, nil, :a), :b),
    #         :bar))
    # to the string "a.b.bar"
    raise Bud::CompileError, "malformed expression: #{exp}" unless exp.sexp_type == :call
    _, recv, op = exp
    return recv.nil? ? op.to_s : call_to_id(recv) + "." + op.to_s
  end

  # We want to distinguish between collection dependencies that occur in
  # top-level expressions versus collections that are referenced inside rule
  # bodies. We just want to set a flag when processing the :iter body, but
  # annoyingly it seems that is hard to do without duplicating the
  # implementation of process_iter().
  #
  # XXX: the whole RuleRewriter approach is wrong because it conflates
  # converting ASTs to strings with doing analysis on ASTs. Those should be
  # split into two separate passes.
  def process_iter(exp)
    # first field of exp is tag; shift it
    exp.shift
    iter = process exp.shift
    args = exp.shift

    @iter_stack.push(true)
    body = exp.empty? ? nil : process(exp.shift)
    @iter_stack.pop

    do_process_iter(iter, args, body)
  end

  def do_process_iter(iter, args, body)
    args = case args
           when 0 then
             " ||"
           else
             a = process(args)[1..-2]
             a = " |#{a}|" unless a.empty?
             a
           end

    b, e = if iter == "END" then
             [ "{", "}" ]
           else
             [ "do", "end" ]
           end

    iter.sub!(/\(\)$/, '')

    # REFACTOR: ugh
    result = []
    result << "#{iter} {"
    result << args
    if body then
      result << " #{body.strip} "
    else
      result << ' '
    end
    result << "}"
    result = result.join
    return result if result !~ /\n/ and result.size < LINE_LENGTH

    result = []
    result << "#{iter} #{b}"
    result << args
    result << "\n"
    if body then
      result << indent(body.strip)
      result << "\n"
    end
    result << e
    result.join
  end

  def process_call(exp)
    tag, recv, op, *args = exp
    if OP_LIST.include?(op) and @context[1] == :defn and @context.length == 2
      # NB: context.length is 2 when see a method call at the top-level of a
      # :defn block -- this is where we expect Bloom statements to appear
      do_rule(exp)
    elsif op == :notin
      # Special case. In the rule "z <= x.notin(y)", z depends positively on x,
      # but negatively on y. See further explanation in the "else" section for
      # why this is a special case.
      if args.first.sexp_type != :call
        raise Bud::CompileError, "illegal argument to notin: #{args.first}"
      end
      notintab = call_to_id(args[0])   # args expected to be of the form (:call nil :y ...)
      @tables[notintab] = true    # "true" denotes non-monotonic dependency
      super
    else
      # Parse a call of the form a.b.c.foo
      #
      # In the most general case, a.b is a nested module, a.b.c is a collection
      # in that module, and a.b.c.foo is either a method or a field. If it is a
      # method, and non-monotonic at that, we register a dependency between lhs
      # and the table a.b.c.  Note that notin is treated differently because in
      # a.b.c.notin(d.e.f), we register a non-monotonic dependency of lhs on
      # "d.e.f", not with "a.b.c"
      ty, qn, _ = exp_id_type(recv, op, args) # qn = qualified name
      if ty == :collection or ty == :lattice
        (@tables[qn] = @nm if @collect) unless @tables[qn]
        @refs_in_body << qn unless @iter_stack.empty?
      #elsif ty == :import .. do nothing
      elsif ty == :not_coll_id
        # Check if receiver is a collection, and further if the current exp
        # represents a field lookup
        op_is_field_name = false
        if recv and recv.first == :call
          rty, _, robj = exp_id_type(recv[1], recv[2], recv[3..-1])
          if rty == :collection
            cols = robj.cols
            op_is_field_name = true if cols and cols.include?(op)
          end
        end
        # For CALM analysis, mark deletion rules as non-monotonic
        @nm = true if op == :-@

        # Don't worry about monotone ops, table names, table.attr calls, or
        # accessors of iterator variables
        if recv
          unless RuleRewriter.is_monotone(op) or op_is_field_name or
                 recv.first == :lvar or op.to_s.start_with?("__")
            @nm = true
          end
        end
      end
      if TEMP_OP_LIST.include? op
        @temp_op = op.to_s.gsub("@", "")
      end
      super
    end
  end

  def self.is_monotone(op)
    MONOTONE_WHITELIST.include?(op) ||
      is_morphism(op) ||
      Bud::Lattice.global_mfuncs.include?(op)
  end

  def self.is_morphism(op)
    Bud::Lattice.global_morphs.include?(op)
  end

  # Rewrite top-level rhs literal expressions into lambdas. During wiring, these
  # are turned into coll_expr collections. For normal relational Bloom, the only
  # literal we expect to see is an array literal, but lattices can be
  # initialized with other kinds of literals (e.g., integers for lmax).
  def lambda_rewrite(rhs)
    # the <= case
    if is_rhs_literal(rhs[0])
      return s(:iter, s(:call, nil, :lambda), s(:args), rhs)
    # the superator case
    elsif rhs[0] == :call \
      and rhs[1] and rhs[1][0] and is_rhs_literal(rhs[1][0]) \
      and rhs[2] and (rhs[2] == :+@ or rhs[2] == :-@ or rhs[2] == :~@)
      return s(rhs[0], s(:iter, s(:call, nil, :lambda), s(:args), rhs[1]), rhs[2], *rhs[3..-1])
    else
      return rhs
    end
  end

  def is_rhs_literal(e)
    [:array, :hash, :lit].include? e
  end

  def collect_rhs(exp)
    exp = lambda_rewrite(exp)

    @collect = true
    rhs = process exp
    @collect = false
    return rhs
  end

  def reset_instance_vars
    @refs_in_body = Set.new
    @tables = {}
    @nm = false
    @temp_op = nil
  end

  def record_rule(lhs, op, rhs_pos, rhs, unsafe_funcs_called)
    rule_txt_orig = "#{lhs} #{op} (#{rhs})"
    rule_txt = "#{lhs} #{op} (#{rhs_pos})"
    if op == :<
      op = "<#{@temp_op}"
    else
      op = op.to_s
    end

    @rules << [@bud_instance, @rule_idx, lhs, op, rule_txt,
               rule_txt_orig, unsafe_funcs_called]
    @tables.each_pair do |t, nm|
      in_rule_body = @refs_in_body.include? t
      @depends << [@bud_instance, @rule_idx, lhs, op, t, nm, in_rule_body]
    end

    reset_instance_vars
    @rule_idx += 1
  end

  def do_rule(exp)
    tag, lhs, op, rhs_ast = exp
    lhs = process(lhs)

    rhs_ast = MapRewriter.new.process(rhs_ast)
    rhs_ast = RenameRewriter.new(@bud_instance).process(rhs_ast)
    rhs_ast = LatticeRefRewriter.new(@bud_instance).process(rhs_ast)
    ufr = UnsafeFuncRewriter.new(@bud_instance)
    rhs_ast = ufr.process(rhs_ast)

    if @bud_instance.options[:no_attr_rewrite]
      rhs = collect_rhs(rhs_ast)
      rhs_pos = rhs
    else
      # need a deep copy of the rules so we can keep a version without AttrName
      # Rewrite
      rhs_ast_dup = Marshal.load(Marshal.dump(rhs_ast))
      rhs = collect_rhs(rhs_ast)
      reset_instance_vars
      rhs_pos = collect_rhs(AttrNameRewriter.new(@bud_instance).process(rhs_ast_dup))
    end
    record_rule(lhs, op, rhs_pos, rhs, ufr.unsafe_func_called)
    drain(exp)
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
  end
end

# We want to rewrite "map" calls on BudCollections to "pro" calls. It is hard
# to do this precisely (issue #225), so we just replace map calls liberally
# and define Enumerable#pro as an alias for "map".
class MapRewriter < SexpProcessor
  def initialize
    super
    self.require_empty = false
    self.expected = Sexp
  end

  def process_call(exp)
    tag, recv, op, *args = exp

    if op == :map and args.empty?
      op = :pro
    end

    s(tag, process(recv), op, *(args.map{|a| process(a)}))
  end
end

# Look for rename statements and define the necessary scratch collections
class RenameRewriter < SexpProcessor
  def initialize(bud_instance)
    super()
    self.require_empty = false
    self.expected = Sexp
    @bud_instance = bud_instance
  end

  def register_scratch(name, schemahash)
    # define a scratch with the name and schema in this rename block
    hash, key_array, val_array = schemahash
    key_array ||= []
    val_array ||= []
    key_cols = key_array.map{|i| i[1] if i.class <= Sexp}.compact
    val_cols = val_array.map{|i| i[1] if i.class <= Sexp}.compact
    @bud_instance.scratch(name, key_cols=>val_cols)
  end

  def process_call(exp)
    tag, recv, op, *args = exp

    if op == :rename
      raise Bud::CompileError, "reduce takes two arguments" unless args.size == 2
      namelit, schemahash = args
      register_scratch(namelit[1], schemahash)
    end

    return s(tag, process(recv), op, *(args.map{|a| process(a)}))
  end
end

# Check for whether the rule invokes any "unsafe" functions (functions that
# might return a different value every time they are called, e.g., budtime). The
# test for "unsafe" functions is pretty naive: any function call with a nil
# receiver is treated as unsafe unless it is belongs to a list of "safe"
# functions (below) or it denotes a lattice identifier. In the latter case, the
# rule is akin to an implicit join with the lattice, so we only rescan it on
# deltas to the lattice (see "rescan_on_merge" in LatticeWrapper).
#
# Although this is called a rewriter, it doesn't modify the input AST.
class UnsafeFuncRewriter < SexpProcessor
  SAFE_FUNC_LIST = [:int_ip_port, :ip_port, :ip, :port].to_set

  attr_reader :unsafe_func_called

  def initialize(bud_instance)
    super()
    self.require_empty = false
    self.expected = Sexp
    @bud_instance = bud_instance
    @unsafe_func_called = false
    @elem_stack = []
  end

  def process_call(exp)
    tag, recv, op, *args = exp

    # We assume that unsafe funcs have a nil receiver (Bud instance is implicit
    # receiver).
    if recv.nil? and @elem_stack.size > 0
      unless is_safe_func(op) || is_collection_name?(op)
        @unsafe_func_called = true
      end
    end

    return s(tag, process(recv), op, *(args.map{|a| process(a)}))
  end

  def process_iter(exp)
    tag, recv, iter_args, body = exp
    if (iter_args == 0)
      iter_args = s(:args)
    end
    new_body = push_and_process(body)
    return s(tag, process(recv), process(iter_args), new_body)
  end

  def push_and_process(exp)
    obj_id = exp.object_id
    @elem_stack.push(obj_id)
    rv = process(exp)
    raise Bud::Error unless @elem_stack.pop == obj_id
    return rv
  end

  def is_collection_name?(op)
    @bud_instance.tables.has_key?(op.to_sym) || @bud_instance.lattices.has_key?(op.to_sym)
  end

  def is_safe_func(op)
    SAFE_FUNC_LIST.include? op
  end
end

# Rewrite references to lattice identifiers that appear in rule bodies. A
# reference to a lattice identifier returns the associated lattice wrapper. When
# the identifier appears at the top-level of the rule RHS, that is fine (since
# we want the wrapper to do wiring). But for references that appear inside rule
# bodies, we want to instead fetch the current value associated with the lattice
# wrapper.
class LatticeRefRewriter < SexpProcessor
  def initialize(bud_instance)
    super()
    self.require_empty = false
    self.expected = Sexp
    @bud_instance = bud_instance
    @elem_stack = []
  end

  def process_iter(exp)
    tag, recv, iter_args, body = exp
    new_body = push_and_process(body)
    if (iter_args == 0)
      iter_args = s(:args)
    end
    return s(tag, process(recv), process(iter_args), new_body)
  end

  def process_array(exp)
    new_body = exp.sexp_body.map {|t| push_and_process(t)}
    return s(:array, *new_body)
  end

  def process_hash(exp)
    new_body = exp.sexp_body.map {|t| push_and_process(t)}
    return s(:hash, *new_body)
  end

  def process_call(exp)
    tag, recv, op, *args = exp

    if recv.nil? and args.empty? and is_lattice?(op) and @elem_stack.size > 0
      return s(:call, exp, :current_value)
    else
      return s(tag, process(recv), op, *(args.map{|a| process(a)}))
    end
  end

  def push_and_process(exp)
    obj_id = exp.object_id
    @elem_stack.push(obj_id)
    rv = process(exp)
    raise Bud::Error unless @elem_stack.pop == obj_id
    return rv
  end

  def is_lattice?(op)
    @bud_instance.lattices.has_key? op.to_sym
  end
end

# Rewrite named-column refs to positional refs
class AttrNameRewriter < SexpProcessor # :nodoc: all
  def initialize(bud_instance)
    super()
    self.require_empty = false
    self.expected = Sexp
    @iterhash ||= {}
    @collnames = []
    @bud_instance = bud_instance
  end

  # some icky special-case parsing to find mapping between collection names and
  # iter vars
  def process_iter(exp)
    if exp[1] and exp[1][0] == :call
      return exp unless exp[2]
      gather_collection_names(exp[1])
      meth_name = exp[1][2]

      # now find iter vars and match up
      if exp[2][0] == :args and @collnames.size == 1 # single-table iter
        if @iterhash[exp[2][1]]
          raise Bud::CompileError, "redefinition of block variable \"#{exp[2][1]}\" not allowed"
        end

        # XXX: The BudChannel#payloads method assigns the correct schema to
        # tuples that pass through it (i.e., it omits the location specifier);
        # hence we don't want to apply the location rewrite to the code block
        # that is passed to payloads(). This is a dirty hack.
        unless meth_name == :payloads
          @iterhash[exp[2][1]] = @collnames[0]
        end
      elsif exp[2][0] == :args and not @collnames.empty? # join iter with lefts/rights
        case meth_name
        when :lefts
          @iterhash[exp[2][1]] = @collnames[0]
        when :rights
          @iterhash[exp[2][1]] = @collnames[1]
        when :reduce
          unless @collnames.length == 1
            raise Bud::CompileError, "reduce should only have one associated collection, but has #{@collnames.inspect}"
          end
          @iterhash[exp[2][1]] = @collnames[0]
        else
          # join
          if @iterhash[exp[2][1]]
            raise Bud::CompileError, "redefinition of block variable \"#{exp[2][1]}\" not allowed"
          end

          @collnames.each_with_index do |c,i|
            next unless exp[2][i+1]
            @iterhash[exp[2][i+1]] = c
          end
        end
      end
    end
    (1..(exp.length-1)).each {|i| exp[i] = process(exp[i])}
    exp
  end

  def gather_collection_names(exp)
    # We expect a reference to a collection name to look like a function call
    # (nil receiver) with no arguments.
    if exp.sexp_type == :call and exp[1].nil? and exp.length == 3
      @collnames << exp[2]
    elsif exp.sexp_type == :call and exp[2] == :rename
      namelit = exp[3]
      @collnames << namelit[1]
    elsif exp.sexp_type == :call and [:group, :argagg].include?(exp[2])
      # For grouping and argagg expressions, only look at the receiver (the
      # collection we're grouping on); otherwise, we might mistakenly think some
      # of the arguments to the grouping operation are collection names.
      gather_collection_names(exp[1])
    else
      exp.each { |e| gather_collection_names(e) if e.class <= Sexp }
    end
  end

  def process_call(exp)
    call, recv, op, *args = exp

    if recv.class == Sexp and recv.sexp_type == :lvar and @iterhash[recv[1]]
      if @bud_instance.respond_to?(@iterhash[recv[1]])
        if @bud_instance.send(@iterhash[recv[1]]).class <= Bud::BudCollection
          cols = @bud_instance.send(@iterhash[recv[1]]).cols
          if op != :[] and @bud_instance.send(@iterhash[recv[1]]).respond_to?(op)
            # if the op is an attribute name in the schema, col_idx is its index
            col_idx = cols.index(op) unless cols.nil?
            unless col_idx.nil?
              op = :[]
              args = [s(:lit, col_idx)]
            end
          end
        end
        return s(call, recv, op, *args)
      end
    end
    return s(call, process(recv), op, *(args.map{|a| process(a)}))
  end
end

# Look for temp declarations and remove the "temp" keyword, yielding code that
# we can safely eval. We also record the set of "temp" collections we've seen,
# and provide a helper method that returns the AST of a state block that
# contains declarations for all those temp tables.
class TempExpander < SexpProcessor # :nodoc: all
  attr_reader :tmp_tables
  attr_accessor :did_work

  TEMP_KEYWORD = :temp

  def initialize
    super()
    self.require_empty = false
    self.expected = Sexp
    @tmp_tables = []
    @did_work = false
  end

  def process_defn(exp)
    tag, name, args, *body = exp
    return exp unless name.to_s =~ /^__bloom__.+/

    body.each_with_index do |n,i|
      # temp declarations are misparsed if the RHS contains certain constructs
      # (e.g., group, "do |f| ... end" rather than "{|f| ... }").  Rewrite to
      # correct the misparsing.
      if n.sexp_type == :iter
        iter_body = n.sexp_body
        new_n = fix_temp_decl(iter_body)
        unless new_n.nil?
          body[i] = n = new_n
          @did_work = true
        end
      end

      _, recv, meth, meth_args = n
      if meth == TEMP_KEYWORD and recv.nil?
        body[i] = rewrite_temp(n)
        @did_work = true
      end
    end
    s(tag, name, args, *body)
  end

  private
  def fix_temp_decl(iter_body)
    if iter_body.first.sexp_type == :call
      call_node = iter_body.first
      _, recv, meth, *meth_args = call_node

      if meth == TEMP_KEYWORD and recv.nil?
        _, lhs, op, rhs = meth_args.first
        new_rhs = s(:iter, rhs, *(iter_body[1..-1]))
        meth_args.first[3] = new_rhs
        return call_node
      end
    end
    return nil
  end

  def rewrite_temp(exp)
    _, recv, meth, *args = exp

    raise Bud::CompileError unless recv.nil?
    nest_call = args.first
    raise Bud::CompileError unless nest_call.sexp_type == :call

    nest_recv, nest_op, *nest_args = nest_call.sexp_body
    unless nest_recv.sexp_type == :lit
      recv_src = Ruby2Ruby.new.process(Marshal.load(Marshal.dump(nest_recv)))
      raise Bud::CompileError, "argument to temp must be a symbol: #{recv_src}"
    end

    tmp_name = nest_recv.sexp_body.first
    @tmp_tables << tmp_name
    new_recv = s(:call, nil, tmp_name)
    return s(:call, new_recv, nest_op, *nest_args)
  end
end
