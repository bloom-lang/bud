require 'rubygems'
require 'ruby2ruby'
require 'set'

class RuleRewriter < Ruby2Ruby # :nodoc: all
  attr_accessor :rule_indx, :rules, :depends

  OP_LIST = Set.new([:<<, :<, :<=])
  TEMP_OP_LIST = Set.new([:-@, :~, :+@])
  MONOTONE_WHITELIST = Set.new([:==, :+, :<=, :-, :<, :>, :*, :~,
                                :pairs, :matches, :combos, :flatten, :new,
                                :lefts, :rights, :map, :flat_map, :pro, :merge,
                                :cols, :key_cols, :val_cols, :payloads, :lambda,
                                :tabname, :ip_port, :port, :ip, :int_ip_port])

  def initialize(seed, bud_instance)
    @bud_instance = bud_instance
    @tables = {}
    @nm = false
    @rule_indx = seed
    @collect = false
    @rules = []
    @depends = []
    @nm_funcs_called = false
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
    return $not_id unless args.size == 1
    ty = $not_id
    if recv
      if recv.first == :call
        # possibly nested reference.
        rty, rqn, robj = exp_id_type(recv[1], recv[2], recv[3]) # rty, rqn, .. = receiver's type, qual name etc.
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
    #       s(:call, s(:call, nil, :a, s(:arglist)), :b, s(:arglist)),
    #         :bar ,
    #         s(:arglist)))
    # to the string "a.b.bar"
    raise "Malformed exp: #{exp}" unless (exp[0] == :call)
    _, recv, op, args = exp
    return recv.nil? ? op.to_s : call_to_id(recv) + "." + op.to_s
  end

  def process_call(exp)
    recv, op, args = exp
    if OP_LIST.include?(op) and @context[1] == :block and @context.length == 4
      # NB: context.length is 4 when see a method call at the top-level of a
      # :defn block -- this is where we expect Bloom statements to appear
      do_rule(exp)
    elsif op == :notin
      # special case. In the rule  "z <= x.notin(y)", z depends positively on x, but negatively on y
      # See further explanation in the "else" section for why this is a special case.
      notintab = call_to_id(args[1])   # args expected to be of the form (:arglist (:call nil :y ...))
      @tables[notintab.to_s] = true # "true" denotes non-monotonic dependency.
      super
    else
      # Parse a call of the form  a.b.c.foo.
      # In the most general case, a.b is a nested module, a.b.c is a collection in that module, and
      # a.b.c.foo is either a method or a field. If it is a method, and non-monotonic at that, we
      # register a dependency between lhs and the table a.b.c.
      # Note that notin is treated differently because in  a.b.c.notin(d.e.f), we register a non-monotonic
      # dependency of lhs on "d.e.f", not with "a.b.c"
      ty, qn, _ = exp_id_type(recv, op, args) # qn = qualified name
      if ty == :collection or ty == :lattice
        (@tables[qn] = @nm if @collect) unless @tables[qn]
      #elsif ty == :import .. do nothing
      elsif ty == :not_coll_id
        # check if receiver is a collection, and further if the current exp
        # represents a field lookup
        op_is_field_name = false
        if recv and recv.first == :call
          rty, _, robj = exp_id_type(recv[1], recv[2], recv[3])
          if rty == :collection
            cols = robj.cols
            op_is_field_name = true if cols and cols.include?(op)
          end
        end
        # for CALM analysis, mark deletion rules as non-monotonic
        @nm = true if op == :-@
        if recv
          # don't worry about monotone ops, table names, table.attr calls, or
          # accessors of iterator variables
          unless RuleRewriter.is_monotone(op) or op_is_field_name or
                 recv.first == :lvar or op.to_s.start_with?("__")
            @nm = true
          end
        else
          # function called (implicit receiver = Bud instance) in a user-defined
          # code block. Check if it is non-monotonic (like budtime, that
          # produces a new value every time it is called)
          @nm_funcs_called = true unless RuleRewriter.is_monotone(op)
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

  # Rewrite top-level rhs array literals to lambdas. During wiring, these are
  # turned into coll_expr collections.
  def lambda_rewrite(rhs)
    # the <= case
    if rhs[0] == :array
      return s(:iter, s(:call, nil, :lambda, s(:arglist)), nil, rhs)
    # the superator case
    elsif rhs[0] == :call \
      and rhs[1] and rhs[1][0] and rhs[1][0] == :array \
      and rhs[2] and (rhs[2] == :+@ or rhs[2] == :-@ or rhs[2] == :~@)
      return s(rhs[0], s(:iter, s(:call, nil, :lambda, s(:arglist)), nil, rhs[1]), rhs[2], rhs[3])
    else
      return rhs
    end
  end

  def collect_rhs(exp)
    exp = lambda_rewrite(exp)

    @collect = true
    rhs = process exp
    @collect = false
    return rhs
  end

  def reset_instance_vars
    @tables = {}
    @nm = false
    @nm_funcs_called = false
    @temp_op = nil
  end

  def record_rule(lhs, op, rhs_pos, rhs)
    rule_txt_orig = "#{lhs} #{op} (#{rhs})"
    rule_txt = "#{lhs} #{op} (#{rhs_pos})"
    if op == :<
      op = "<#{@temp_op}"
    else
      op = op.to_s
    end

    @rules << [@bud_instance, @rule_indx, lhs, op, rule_txt, rule_txt_orig, @nm_funcs_called]
    @tables.each_pair do |t, nm|
      @depends << [@bud_instance, @rule_indx, lhs, op, t, nm]
    end

    reset_instance_vars
    @rule_indx += 1
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    rhs_ast = map2pro(exp[2])

    # Remove the outer s(:arglist) from the rhs AST. An AST subtree rooted with
    # s(:arglist) is not really sensible and it causes Ruby2Ruby < 1.3.1 to
    # misbehave (for example, s(:arglist, s(:hash, ...)) is misparsed.
    raise Bud::CompileError unless rhs_ast.sexp_type == :arglist
    rhs_ast = rhs_ast[1]

    rhs_ast = RenameRewriter.new(@bud_instance).process(rhs_ast)

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
    record_rule(lhs, op, rhs_pos, rhs)
    drain(exp)
  end

  # We want to rewrite "map" calls on BudCollections to "pro" calls. It is hard
  # to do this precisely (issue #225), so we just replace map calls liberally
  # and define Enumerable#pro as an alias for "map".
  def map2pro(exp)
    # the non-superator case
    if exp[1] and exp[1][0] and exp[1][0] == :iter \
      and exp[1][1] and exp[1][1][1] and exp[1][1][1][0] == :call
      if exp[1][1][2] == :map
        exp[1][1][2] = :pro
      end
    # the superator case
    elsif exp[1] and exp[1][0] == :call and (exp[1][2] == :~@ or exp[1][2] == :+@ or exp[1][2] == :-@)
      if exp[1][1] and exp[1][1][1] and exp[1][1][1][2] == :map
        exp[1][1][1][2] = :pro
      end
    end
    exp
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
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
    call, recv, op, args = exp

    if op == :rename
      arglist, namelit, schemahash = args
      register_scratch(namelit[1], schemahash)
    end

    return s(call, process(recv), op, process(args))
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
      gather_collection_names(exp[1])

      # now find iter vars and match up
      if exp[2] and exp[2][0] == :lasgn and @collnames.size == 1 #single-table iter
        raise Bud::CompileError, "nested redefinition of block variable \"#{exp[2][1]}\" not allowed" if @iterhash[exp[2][1]]
        @iterhash[exp[2][1]] = @collnames[0]
      elsif exp[2] and exp[2][0] == :lasgn and @collnames.size > 1 and exp[1] # join iter with lefts/rights
        case exp[1][2]
        when :lefts
          @iterhash[exp[2][1]] = @collnames[0]
        when :rights
          @iterhash[exp[2][1]] = @collnames[1]
        else
          raise Bud::CompileError, "nested redefinition of block variable \"#{exp[2][1]}\" not allowed" if @iterhash[exp[2][1]]
        end
      elsif exp[2] and exp[2][0] == :masgn and not @collnames.empty? # join or reduce iter
        return unless exp[2][1] and exp[2][1][0] == :array
        if exp[1][2] == :reduce
          unless @collnames.length == 1
            raise Bud::Error, "reduce should only have one associated collection, but has #{@collnames.inspect}"
          end
          @iterhash[exp[2][1][2][1]] = @collnames.first
        else #join
          @collnames.each_with_index do |c, i|
            next unless exp[2][1][i+1] and exp[2][1][i+1][0] == :lasgn
            @iterhash[exp[2][1][i+1][1]] = c
          end
        end
      end
    end
    (1..(exp.length-1)).each {|i| exp[i] = process(exp[i])}
    exp
  end

  def gather_collection_names(exp)
    if exp[0] == :call and exp[1].nil?
      @collnames << exp[2]
    elsif exp[2] and exp[2] == :rename
      arglist, namelit, schemahash = exp[3]
      @collnames << namelit[1]
    else
      exp.each { |e| gather_collection_names(e) if e and e.class <= Sexp }
    end
  end

  def process_call(exp)
    call, recv, op, args = exp

    if recv and recv.class == Sexp and recv.first == :lvar and recv[1] and @iterhash[recv[1]]
      if @bud_instance.respond_to?(@iterhash[recv[1]])
        if @bud_instance.send(@iterhash[recv[1]]).class <= Bud::BudCollection
          cols = @bud_instance.send(@iterhash[recv[1]]).cols
          if op != :[] and @bud_instance.send(@iterhash[recv[1]]).respond_to?(op)
            # if the op is an attribute name in the schema, col is its index
            col = cols.index(op) unless cols.nil?
            unless col.nil?
              op = :[]
              args = s(:arglist, s(:lit, col))
            end
          end
        end
        return s(call, recv, op, args)
      end
    end
    return s(call, process(recv), op, process(args))
  end
end

# Look for temp declarations and remove the "temp" keyword, yielding code that
# we can safely eval. We also record the set of "temp" collections we've seen,
# and provide a helper method that returns the AST of a state block that
# contains declarations for all those temp tables.
class TempExpander < SexpProcessor # :nodoc: all
  attr_reader :tmp_tables
  attr_accessor :did_work

  KEYWORD = :temp

  def initialize
    super()
    self.require_empty = false
    self.expected = Sexp
    @tmp_tables = []
    @did_work = false
  end

  def process_defn(exp)
    tag, name, args, scope = exp
    if name.to_s =~ /^__bloom__.+/
      block = scope[1]

      block.each_with_index do |n,i|
        if i == 0
          raise Bud::CompileError if n != :block
          next
        end

        # temp declarations are misparsed if the RHS contains certain constructs
        # (e.g., group, "do |f| ... end" rather than "{|f| ... }").  Rewrite to
        # correct the misparsing.
        if n.sexp_type == :iter
          iter_body = n.sexp_body
          new_n = fix_temp_decl(iter_body)
          unless new_n.nil?
            block[i] = n = new_n
            @did_work = true
          end
        end

        _, recv, meth, meth_args = n
        if meth == KEYWORD and recv.nil?
          block[i] = rewrite_me(n)
          @did_work = true
        end
      end
    end
    s(tag, name, args, scope)
  end

  private
  def fix_temp_decl(iter_body)
    if iter_body.first.sexp_type == :call
      call_node = iter_body.first

      _, recv, meth, meth_args = call_node
      if meth == KEYWORD and recv.nil?
        _, lhs, op, rhs = meth_args.sexp_body.first

        old_rhs_body = rhs.sexp_body
        new_rhs_body = [:iter]
        new_rhs_body += old_rhs_body
        new_rhs_body += iter_body[1..-1]
        rhs[1] = Sexp.from_array(new_rhs_body)
        return call_node
      end
    end
    return nil
  end

  def rewrite_me(exp)
    _, recv, meth, args = exp

    raise Bud::CompileError unless recv == nil
    nest_call = args.sexp_body.first
    raise Bud::CompileError unless nest_call.sexp_type == :call

    nest_recv, nest_op, nest_args = nest_call.sexp_body
    raise Bud::CompileError unless nest_recv.sexp_type == :lit

    tmp_name = nest_recv.sexp_body.first
    @tmp_tables << tmp_name
    new_recv = s(:call, nil, tmp_name, s(:arglist))
    return s(:call, new_recv, nest_op, nest_args)
  end
end
