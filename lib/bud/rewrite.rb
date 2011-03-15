require 'rubygems'
require 'ruby2ruby'

class RuleRewriter < Ruby2Ruby
  attr_accessor :rule_indx, :rules, :depends

  def initialize(seed)
    @ops = {:<< => 1, :< => 1, :<= => 1}
    @monotonic_whitelist = {:== => 1, :+ => 1, :- => 1, :<= => 1, :- => 1, :< => 1, :> => 1}
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
    if exp[0].nil? and exp[2] == s(:arglist) and @collect
      do_table(exp)
    elsif @ops[exp[1]] and @context[1] == :block and @context.length == 4
      # NB: context.length is 4 when see a method call at the top-level of a
      # :defn block -- this is where we expect Bloom ops to appear
      do_rule(exp)
    else
      if exp[0] and exp[0].class == Sexp
        # ignore accessors of iterator variables
        unless exp[0].first == :lvar
          if exp[2].class == Sexp and exp[2].length == 1 and exp[2] == s(:arglist)
            # check for delete op, but ignore top-level accessors and maps
            @nm = true if exp[1] == :-@
          else
            unless @monotonic_whitelist[exp[1]]
              # suspicious function: exp[1]
              @nm = true
            end
          end
        end
      end
      if @temp_ops[exp[1]]
        @temp_op = exp[1].to_s.gsub("@", "")
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
    rule_txt = "#{lhs} #{op} #{rhs}"
    if op == :<
      op = "<#{@temp_op}"
    else
      op = op.to_s
    end

    @rules << [@rule_indx, lhs, op, rule_txt]
    @tables.each_pair do |k, v|
      @depends << [@rule_indx, lhs, op, k, v]
    end

    @tables = {}
    @nm = false
    @temp_op = nil
    @rule_indx += 1
  end

  def do_table(exp)
    t = exp[1].to_s
    @tables[t] = @nm
    drain(exp)
    return t
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    rhs = collect_rhs(exp[2])
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
  end
end

class StateExtractor < Ruby2Ruby
  attr_reader :decls

  def initialize(context)
    @cxt = context
    @decls = []
    super()
  end

  def process_call(exp)
    lhs = process exp[2]
    foo = "#{exp[1]} #{lhs}"
    @decls << ["#{lhs}"[/:.*?,/][1..-1].chop!, foo]
    exp.shift until exp.empty?
    return ""
  end
end

# Perform macro expansion on a tree of Sexps.
class VarRewriter < SexpProcessor
  def initialize(var_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @var_tbl = var_tbl
  end

  # Return a list of all variables k such that k appears in a "s(:lasgn, :k)"
  # node in the given Sexp tree.
  def find_lasgn_vars(e)
    return [] if e.nil?

    if e.sexp_type == :lasgn
      return [e.sexp_body.first]
    end

    rv = []
    e.sexp_body.each do |s|
      if Array === s
        rv.concat(find_lasgn_vars s)
      end
    end
    rv
  end

  # Check whether any of the variables introduced in this block shadow a
  # variable we are rewriting. If so, temporarily stop rewriting that variable,
  # process the block, and then re-add the variable to the rewriting table.
  def process_iter(exp)
    tag, iter, args, body = exp

    shadow_vars = {}
    vars = find_lasgn_vars(args)
    vars.each do |v|
      if @var_tbl.has_key? v
        shadow_vars[v] = @var_tbl.delete(v)
      end
    end

    result = []
    until exp.empty?
      sub_exp = exp.shift
      if Array === sub_exp
        result << process(sub_exp)
      else
        result << sub_exp
      end
    end

    # assert: no duplicate keys inserted after recursion
    raise Bud::CompileError unless (@var_tbl.keys & shadow_vars.keys).empty?
    @var_tbl.merge! shadow_vars

    Sexp.from_array(result)
  end

  def process_lvar(exp)
    var_name = exp[1]
    if @var_tbl.has_key? var_name
      expansion = @var_tbl[var_name]
      # NB: We need to return a deep copy of the macro expansion. This is
      # because subsequent sexp processing is destructive -- we don't want
      # mutations performed to one expansion of a macro to effect other uses of
      # the macro. Apparently this is the best way to do a deep copy in Ruby.
      return Marshal.load(Marshal.dump(expansion))
    end

    return exp
  end
end

# Given a table of renames from x => y, replace all calls to "x" with calls to
# "y" instead. Unlike VarRewriter, we don't try to handle shadowing due to block
# variables: if a block references a block variable that shadows an identifier
# in the rename tbl, it should appear as an :lvar node rather than a :call, so
# we should be okay.
# XXX: If this module imports a submodule :p and we see a call to p.x, we
# shouldn't try to rewrite x.
class CallRewriter < SexpProcessor
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

    Sexp.from_array [tag, recv, meth_name, args]
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
class NestedRefRewriter < SexpProcessor
  def initialize(import_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @import_tbl = import_tbl
  end

  def process_call(exp)
    tag, recv, meth_name, args = exp

    do_lookup, recv_stack = make_recv_stack(recv)
    did_rewrite = false

    if do_lookup and recv_stack.length > 0
      lookup_tbl = @import_tbl
      do_rewrite = true
      new_meth_name = ""
      until recv_stack.empty?
        m = recv_stack.pop

        unless lookup_tbl.has_key? m
          do_rewrite = false
          break
        end

        new_meth_name += "#{m}__"
        lookup_tbl = lookup_tbl[m]
      end

      if do_rewrite
        new_meth_name += meth_name.to_s
        recv = nil
        meth_name = new_meth_name.to_sym
        did_rewrite = true
      end
    end

    recv = process(recv)
    args = process(args)

    Sexp.from_array [tag, recv, meth_name, args]
  end

  def make_recv_stack(r)
    rv = []

    while true
      break if r.nil?
      # We can exit early if we see something unexpected
      return [false, []] unless r.sexp_type == :call

      recv, meth_name, args = r.sexp_body
      unless args.sexp_type == :arglist and args.sexp_body.length == 0
        return [false, []]
      end

      rv << meth_name
      r = recv
    end

    return [true, rv]
  end
end

class DefnRenamer < SexpProcessor
  def initialize(old_mod_name, new_mod_name)
    super()
    self.require_empty = false
    self.expected = Sexp
    @old_mod_name = old_mod_name
    @new_mod_name = new_mod_name
  end

  def process_defn(exp)
    tag, name, args, scope = exp

    if name.to_s == "__#{@old_mod_name}__bootstrap"
      name = "__#{@new_mod_name}__bootstrap".to_sym
    elsif name.to_s == "__#{@old_mod_name}__state"
      name = "__#{@new_mod_name}__state".to_sym
    end

    # Note that we don't bother to recurse further into the AST: we're only
    # interested in top-level :defn nodes, anyway.
    Sexp.from_array [tag, name, args, scope]
  end
end

module ModuleRewriter
  # Do the heavy-lifting to import the Bloom module "mod" into the class/module
  # "import_site", bound to "local_name" at the import site. We implement this
  # by converting the importered module into an AST, and then rewriting the AST
  # so that (a) state defined by the module is mangled to include the local bind
  # name (b) statements in the module are rewritten to reference the mangled
  # names (c) statements in the module that reference sub-modules are rewritten
  # to reference the mangled name of the submodule. We then convert the
  # rewritten AST back into Ruby source text using Ruby2Ruby and eval() it, to
  # define a new module. We return the name of that newly defined module; the
  # caller can then use "include" to load the module into the import site.
  def self.do_import(import_site, mod, local_name)
    raise Bud::BudError unless (mod.class <= Module and local_name.class <= Symbol)

    ast = get_module_ast(mod)
    ast, new_mod_name = ast_rename_module(ast, import_site, mod, local_name)
    rename_tbl = ast_rename_state(ast, import_site, mod, local_name)
    ast = ast_update_refs(ast, rename_tbl)
    ast = ast_flatten_nested_refs(ast, mod.bud_import_table)

    r2r = Ruby2Ruby.new
    str = r2r.process(ast)

    rv = import_site.module_eval str
    raise Bud::BudError unless rv.nil?
    return new_mod_name
  end

  def self.get_module_ast(mod)
    u = Unifier.new
    u.process(ParseTree.translate(mod))
  end

  # Rename the given module's AST to be a mangle of import site, imported
  # module, and local bind name. We also need to rename special methods whose
  # name contains the name of the module: the (metaprogrammed) state and
  # bootstrap instance methods.
  def self.ast_rename_module(ast, importer, importee, local_name)
    raise Bud::BudError unless ast.sexp_type == :module

    mod_name = ast.sexp_body.first
    raise Bud::BudError if mod_name.to_s != importee.to_s

    new_name = "#{importer}__#{importee}__#{local_name}"
    ast[1] = new_name.to_sym

    dr = DefnRenamer.new(mod_name, new_name)
    new_ast = dr.process(ast)

    # XXX: it would be nice to return a Module, rather than a string containing
    # the Module's name. Unfortunately, I can't see how to do that.
    return [new_ast, new_name]
  end

  # Mangle the names of all the collections defined in state blocks found in the
  # given module's AST.
  def self.ast_rename_state(ast, importer, importee, local_name)
    # Find all the state blocks in the AST
    raise Bud::BudError unless ast.sexp_type == :module

    rename_tbl = {}
    ast.sexp_body.each do |b|
      next unless b.class <= Sexp
      next if b.sexp_type != :defn

      def_name, args, scope = b.sexp_body
      next unless /__.+?__state$/.match def_name.to_s

      raise Bud::BudError unless scope.sexp_type == :scope
      state_block = scope.sexp_body.first
      raise Bud::BudError unless state_block.sexp_type == :block
      next unless state_block.sexp_body

      state_block.sexp_body.each do |e|
        raise Bud::BudError unless e.sexp_type == :call

        recv, meth_name, args = e.sexp_body
        raise Bud::BudError unless args.sexp_type == :arglist
        first_arg = args.sexp_body.first
        raise Bud::BudError unless first_arg.sexp_type == :lit
        tbl_name = first_arg.sexp_body.first

        new_tbl_name = "#{local_name}__#{tbl_name}".to_sym
        rename_tbl[tbl_name] = new_tbl_name

        first_arg[1] = new_tbl_name
      end
    end

    return rename_tbl
  end

  def self.ast_update_refs(ast, rename_tbl)
    cr = CallRewriter.new(rename_tbl)
    cr.process(ast)
  end

  # If this module imports a submodule and binds it to :x, references to x.t1
  # need to be flattened to the mangled name of x.t1.
  def self.ast_flatten_nested_refs(ast, import_tbl)
    nr = NestedRefRewriter.new(import_tbl)
    nr.process(ast)
  end

  # Return a list of symbols containing the names of def blocks containing Bloom
  # rules in the given module and all of its ancestors.
  def self.get_rule_defs(mod)
    rv = []

    mod.ancestors.each do |anc|
      next unless anc.methods.include? "annotation"
      rv += anc.annotation.map{|a| a[0] if a[1].keys.include? :declare}
    end

    rv.compact.uniq
  end
end
