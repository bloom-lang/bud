require 'rubygems'
require 'ruby2ruby'

class RuleRewriter < Ruby2Ruby # :nodoc: all 
  attr_accessor :rule_indx, :rules, :depends

  def initialize(seed, bud_instance)
    @bud_instance = bud_instance
    @ops = {:<< => 1, :< => 1, :<= => 1}
    @monotonic_whitelist = {
          :== => 1, :+ => 1, :<= => 1, :- => 1, :< => 1, :> => 1,
          :* => 1, :pairs => 1, :matches => 1, :combos => 1, :flatten => 1,
          :lefts => 1, :rights => 1, :map => 1, :flat_map => 1, :pro => 1,
          :schema => 1, :keys => 1, :values => 1, :payloads => 1
      }
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
    recv, op, args = exp
    if recv.nil? and args == s(:arglist) and @collect
      do_table(exp)
    elsif @ops[op] and @context[1] == :block and @context.length == 4
      # NB: context.length is 4 when see a method call at the top-level of a
      # :defn block -- this is where we expect Bloom statements to appear
      do_rule(exp)
    else
      if recv and recv.class == Sexp
        # ignore accessors of iterator variables
        unless recv.first == :lvar
          @nm = true if op == :-@
          @nm = true unless (@monotonic_whitelist[op] or @bud_instance.tables.has_key? op)
        end
      end
      if @temp_ops[op]
        @temp_op = op.to_s.gsub("@", "")
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
    rule_txt = "#{lhs} #{op} (#{rhs})"
    if op == :<
      op = "<#{@temp_op}"
    else
      op = op.to_s
    end

    @rules << [@rule_indx, lhs, op, rule_txt]
    @tables.each_pair do |t, non_monotonic|
      @depends << [@rule_indx, lhs, op, t, non_monotonic]
    end

    @tables = {}
    @nm = false
    @temp_op = nil
    @rule_indx += 1
  end

  def do_table(exp)
    t = exp[1].to_s
    # If we're called on a "table-like" part of the AST that doesn't correspond
    # to an extant table, ignore it.
    @tables[t] = @nm if @bud_instance.tables.has_key? t.to_sym
    drain(exp)
    return t
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    rhs = collect_rhs(map2pro(exp[2]))
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  # Look for top-level map on a base-table on rhs, and rewrite to pro
  def map2pro(exp)
    if exp[1] and exp[1][0] and exp[1][0] == :iter \
       and exp[1][1] and exp[1][1][1] == :call \
       and exp[1][1][2] == :map
      exp[1][1][2] = :pro
    end
    exp
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
  end
end

# Given a table of renames from x => y, replace all calls to "x" with calls to
# "y" instead. We don't try to handle shadowing due to block variables: if a
# block references a block variable that shadows an identifier in the rename
# tbl, it should appear as an :lvar node rather than a :call, so we should be
# okay.
class CallRewriter < SexpProcessor # :nodoc: all
  def initialize(rename_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @rename_tbl = rename_tbl
  end

  def process_call(exp)
    tag, recv, meth_name, args = exp

    if @rename_tbl.has_key? meth_name
      meth_name = @rename_tbl[meth_name] # No need to deep-copy Symbol
    end

    recv = process(recv)
    args = process(args)

    s(tag, recv, meth_name, args)
  end
end

# Rewrite qualified references to collections defined by an imported module. In
# the AST, this looks like a tree of :call nodes. For example, a.b.c looks like:
#
#   (:call, (:call, (:call, nil, :a, args), :b, args), :c, args)
#
# If the import table contains [a][b], we want to rewrite this into a single
# call to a__b__c, which matches how the corresponding Bloom collection will
# be name-mangled. Note that we don't currently check that a__b__c (or a.b.c)
# corresponds to an extant Bloom collection.
class NestedRefRewriter < SexpProcessor # :nodoc: all
  attr_accessor :did_work

  def initialize(import_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @import_tbl = import_tbl
    @did_work = false
  end

  def process_call(exp)
    return exp if @import_tbl.empty?
    tag, recv, meth_name, args = exp

    catch :skip do
      recv_stack = make_recv_stack(recv)
      throw :skip unless recv_stack.length > 0

      lookup_tbl = @import_tbl
      new_meth_name = ""
      until recv_stack.empty?
        m = recv_stack.pop
        throw :skip unless lookup_tbl.has_key? m

        new_meth_name += "#{m}__"
        lookup_tbl = lookup_tbl[m]
      end

      # Okay, apply the rewrite
      @did_work = true
      new_meth_name += meth_name.to_s
      recv = nil
      meth_name = new_meth_name.to_sym
    end

    recv = process(recv)
    args = process(args)

    s(tag, recv, meth_name, args)
  end

  private
  def make_recv_stack(r)
    rv = []

    while true
      break if r.nil?
      # We can exit early if we see something unexpected
      throw :skip unless r.sexp_type == :call

      recv, meth_name, args = r.sexp_body
      unless args.sexp_type == :arglist and args.sexp_body.length == 0
        throw :skip
      end

      rv << meth_name
      r = recv
    end

    return rv
  end
end

# Look for temp declarations and remove the "temp" keyword, yielding code that
# we can safely eval. We also record the set of "temp" collections we've seen,
# and provide a helper method that returns the AST of a state block that
# contains declarations for all those temp tables.
class TempExpander < SexpProcessor # :nodoc: all
  attr_reader :tmp_tables
  attr_accessor :did_work

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

          if iter_body.first.sexp_type == :call
            call_node = iter_body.first

            _, recv, meth, meth_args = call_node
            if meth == :temp and recv.nil?
              _, lhs, op, rhs = meth_args.sexp_body.first

              old_rhs_body = rhs.sexp_body
              rhs[1] = s(:iter)
              rhs[1] += old_rhs_body
              rhs[1] += iter_body[1..-1]
              block[i] = n = call_node
              @did_work = true
            end
          end
        end

        _, recv, meth, meth_args = n
        if meth == :temp and recv.nil?
          block[i] = rewrite_temp(n)
          @did_work = true
        end
      end
    end

    s(tag, name, args, scope)
  end

  def get_state_meth(klass)
    return if @tmp_tables.empty?
    block = s(:block)

    @tmp_tables.each do |t|
      args = s(:arglist, s(:lit, t.to_sym))
      block << s(:call, nil, :temp, args)
    end

    meth_name = Module.make_state_meth_name(klass).to_s + "__tmp"
    return s(:defn, meth_name.to_sym, s(:args), s(:scope, block))
  end

  private
  def rewrite_temp(exp)
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

class DefnRenamer < SexpProcessor # :nodoc: all
  def initialize(old_mod_name, new_mod_name, local_name)
    super()
    self.require_empty = false
    self.expected = Sexp
    @old_mod_name = old_mod_name
    @new_mod_name = new_mod_name
    @local_name = local_name
  end

  def process_defn(exp)
    tag, name, args, scope = exp
    name_s = name.to_s

    if name_s =~ /^__bootstrap__.+$/
      name = name_s.sub(/^(__bootstrap__)(.+)$/, "\\1#{@local_name}__\\2").to_sym
    elsif name_s =~ /^__state\d+__/
      name = name_s.sub(/^(__state\d+__)(.*)$/, "\\1#{@local_name}__\\2").to_sym
    elsif name_s =~ /^__bloom__.+$/
      name = name_s.sub(/^(__bloom__)(.+)$/, "\\1#{@local_name}__\\2").to_sym
    else
      name = "#{@local_name}__#{name_s}".to_sym
    end

    # Note that we don't bother to recurse further into the AST: we're only
    # interested in top-level :defn nodes.
    s(tag, name, args, scope)
  end
end

module ModuleRewriter # :nodoc: all
  # Do the heavy-lifting to import the Bloom module "mod" into the class/module
  # "import_site", bound to "local_name" at the import site. We implement this
  # by converting the imported module into an AST and rewriting the AST like so:
  #
  #   (a) the module name is mangled to include the local bind name and the
  #       importer
  #   (b) instance method names are mangled to include the local bind name
  #   (c) state defined by the module is mangled to include the local bind name
  #   (d) statements in the module are rewritten to reference the mangled names
  #   (e) statements in the module that reference sub-modules are rewritten to
  #       reference the mangled name of the submodule.
  #
  # We then convert the rewritten AST back into Ruby source code using Ruby2Ruby
  # and eval() it to define a new module. We return the name of that newly
  # defined module; the caller can then use "include" to load the module into
  # the import site. Note that additional rewrites are needed to ensure that
  # code in the import site that accesses module contents does the right thing;
  # see Bud#rewrite_local_methods.
  def self.do_import(import_site, mod, local_name)
    ast = get_module_ast(mod)
    ast = ast_flatten_nested_refs(ast, mod.bud_import_table)
    ast = ast_process_temps(ast, mod)
    ast, new_mod_name = ast_rename_module(ast, import_site, mod, local_name)
    rename_tbl = ast_rename_state(ast, local_name)
    ast = ast_update_refs(ast, rename_tbl)

    str = Ruby2Ruby.new.process(ast)
    rv = import_site.module_eval str
    raise Bud::BudError unless rv.nil?

    return new_mod_name
  end

  def self.get_module_ast(mod)
    raw_ast = get_raw_parse_tree(mod)
    unless raw_ast.first == :module
      raise Bud::BudError, "import must be used with a Module"
    end

    return Unifier.new.process(raw_ast)
  end

  # Returns the AST for the given module (as a tree of Sexps). ParseTree
  # provides native support for doing this, but we choose to do it ourselves. In
  # ParseTree <= 3.0.7, the support is buggy; in later versions of ParseTree,
  # the AST is returned in a different format than we expect. In particular, we
  # expect that the methods from any modules included in the target module will
  # be "inlined" into the dumped AST; ParseTree > 3.0.7 adds an "include"
  # statement to the AST instead. In the long run we should adapt the module
  # rewrite system to work with ParseTree > 3.0.7 and get rid of this code, but
  # that will require further changes.
  def self.get_raw_parse_tree(klass)
    pt = RawParseTree.new(false)
    klassname = klass.name
    klassname = klassname.to_sym

    code = if Class === klass then
             sc = klass.superclass
             sc_name = ((sc.nil? or sc.name.empty?) ? "nil" : sc.name).intern
             [:class, klassname, [:const, sc_name]]
           else
             [:module, klassname]
           end

    method_names = klass.private_instance_methods false
    # protected methods are included in instance_methods, go figure!

    # Get the set of classes/modules that define instance methods we want to
    # include in the result
    relatives = klass.modules + [klass]
    relatives.each do |r|
      method_names += r.instance_methods false
    end

    # For each distinct method name, use the implementation that appears the
    # furthest down in the inheritance hierarchy.
    relatives.reverse!
    method_names.uniq.sort.each do |m|
      relatives.each do |r|
        t = pt.parse_tree_for_method(r, m.to_sym)
        if t != [nil]
          code << t
          break
        end
      end
    end

    klass.singleton_methods(false).sort.each do |m|
      code << pt.parse_tree_for_method(klass, m.to_sym, true)
    end

    return code
  end

  # If this module imports a submodule and binds it to :x, references to x.t1
  # need to be flattened to the mangled name of x.t1.
  def self.ast_flatten_nested_refs(ast, import_tbl)
    NestedRefRewriter.new(import_tbl).process(ast)
  end

  # Handle temp collections defined in the module's Bloom blocks.
  def self.ast_process_temps(ast, mod)
    t = TempExpander.new
    ast = t.process(ast)

    new_meth = t.get_state_meth(mod)
    if new_meth
      # Insert the new extra state method into the module's AST
      ast << new_meth
    end

    return ast
  end

  # Rename the given module's name to be a mangle of import site, imported
  # module, and local bind name. We also need to rename special "state" and
  # "bootstrap" methods. We also rename "bloom" methods, but we can just mangle
  # with the local bind name for those.
  def self.ast_rename_module(ast, importer, importee, local_name)
    mod_name = ast.sexp_body.first
    raise Bud::BudError if mod_name.to_s != importee.to_s

    # If the importer or importee modules are nested inside an outer module,
    # strip off the outer module name before using for name mangling purposes
    importer_name = Module.get_class_name(importer)
    importee_name = Module.get_class_name(importee)
    new_name = "#{importer_name}__#{importee_name}__#{local_name}"
    ast[1] = new_name.to_sym

    dr = DefnRenamer.new(mod_name, new_name, local_name)
    new_ast = dr.process(ast)

    # XXX: it would be nice to return a Module, rather than a string containing
    # the Module's name. Unfortunately, I can't see how to do that.
    return [new_ast, new_name]
  end

  # Mangle the names of all the collections defined in state blocks found in the
  # given module's AST. Returns a table mapping old => new names.
  def self.ast_rename_state(ast, local_name)
    # Find all the state blocks in the AST
    raise Bud::BudError unless ast.sexp_type == :module

    rename_tbl = {}
    ast.sexp_body.each do |b|
      next unless b.class <= Sexp
      next if b.sexp_type != :defn

      def_name, args, scope = b.sexp_body
      next unless /^__state\d+__/.match def_name.to_s

      raise Bud::BudError unless scope.sexp_type == :scope
      state_block = scope.sexp_body.first
      raise Bud::BudError unless state_block.sexp_type == :block
      next unless state_block.sexp_body

      # Look for collection definition statements inside the block
      state_block.sexp_body.each do |e|
        raise Bud::BudError unless e.sexp_type == :call

        recv, meth_name, args = e.sexp_body
        raise Bud::BudError unless args.sexp_type == :arglist

        if meth_name == :interface
          tbl_name_node = args.sexp_body[1]
        else
          tbl_name_node = args.sexp_body[0]
        end

        raise Bud::BudError unless tbl_name_node.sexp_type == :lit
        tbl_name = tbl_name_node.sexp_body.first

        new_tbl_name = "#{local_name}__#{tbl_name}".to_sym
        rename_tbl[tbl_name] = new_tbl_name

        tbl_name_node[1] = new_tbl_name
      end
    end

    return rename_tbl
  end

  def self.ast_update_refs(ast, rename_tbl)
    CallRewriter.new(rename_tbl).process(ast)
  end

  # Return a list of symbols containing the names of def blocks containing Bloom
  # rules in the given module and all of its ancestors.
  def self.get_rule_defs(mod)
    mod.instance_methods.select {|m| m =~ /^__bloom__.+$/}
  end
end
