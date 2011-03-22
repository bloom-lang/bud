require 'rubygems'
require 'ruby2ruby'

class RuleRewriter < Ruby2Ruby
  attr_accessor :rule_indx, :rules, :depends, :bud_instance

  def initialize(seed, bud_instance)
    @bud_instance = bud_instance
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
        # ignore accessors of iterator variables, 
        # but do analyze variables from equality rules that got turned into temps!
        if exp[0].first != :lvar or @bud_instance.tables.include? exp[0][1]
          if exp[2].class == Sexp and exp[2].length == 1 and exp[2] == s(:arglist)
            # check for delete ops and predicate methods (ending in "?" like "empty?"), 
            # but ignore top-level accessors and maps
            # XXX we should be more methodical about white/black-listing unary Enumerator 
            # methods, as this will silently fail to notice non-monotonicity if we're wrong.
            @nm = true if exp[1] == :-@ or exp[1].to_s[-1..-1] == '?'
          else
            unless @monotonic_whitelist[exp[1]]
              # suspicious function: exp[1]
              @nm = true
            end
          end
          # now check for variables from equality rules that we converted into temps
          # and register in @tables for dependency checking
          if exp[0].first == :lvar and @bud_instance.tables.include? exp[0][1]
            @tables[exp[0][1].to_s] = @nm
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
    rule_txt = "#{lhs} #{op} (#{rhs})"
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
    if exp[0][2] == :temp
      lhs = handle_temp(exp[0])
    else
      lhs = exp[0]
    end
    lhs = process lhs
    op = exp[1]
    rhs = collect_rhs(map2pro(exp[2]))
    record_rule(lhs, op, rhs)
    drain(exp)
  end

  def handle_temp(lhs)
    raise Bud::CompileError,  "lhs of temp rule not a symbol" unless lhs[3][1][0] == :lit
    temp_name = lhs[3][1][1]
    bud_instance.temp temp_name
    return s(:call, nil, temp_name, s(:arglist))
  end

  # look for top-level map on a base-table on rhs, and rewrite to pro
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

# Given a table of renames from x => y, replace all calls to "x" with calls to
# "y" instead. We don't try to handle shadowing due to block variables: if a
# block references a block variable that shadows an identifier in the rename
# tbl, it should appear as an :lvar node rather than a :call, so we should be
# okay.
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
      new_meth_name += meth_name.to_s
      recv = nil
      meth_name = new_meth_name.to_sym
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

class DefnRenamer < SexpProcessor
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
    elsif name_s =~ /^__state__.+$/
      name = name_s.sub(/^(__state__)(.+)$/, "\\1#{@local_name}__\\2").to_sym
    elsif name_s =~ /^__bloom__.+$/
      name = name_s.sub(/^(__bloom__)(.+)$/, "\\1#{@local_name}__\\2").to_sym
    else
      name = "#{@local_name}__#{name_s}".to_sym
    end

    # Note that we don't bother to recurse further into the AST: we're only
    # interested in top-level :defn nodes.
    Sexp.from_array [tag, name, args, scope]
  end
end

module ModuleRewriter
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
  # the import site.
  def self.do_import(import_site, mod, local_name)
    ast = get_module_ast(mod)
    ast, new_mod_name = ast_rename_module(ast, import_site, mod, local_name)
    rename_tbl = ast_rename_state(ast, local_name)
    ast = ast_update_refs(ast, rename_tbl)
    ast = ast_flatten_nested_refs(ast, mod.bud_import_table)

    str = Ruby2Ruby.new.process(ast)
    rv = import_site.module_eval str
    raise Bud::BudError unless rv.nil?

    return new_mod_name
  end

  def self.get_module_ast(mod)
    raw_ast = ParseTree.translate(mod)
    unless raw_ast.first == :module
      raise Bud::BudError, "import must be used with a Module"
    end

    # XXX: Kludgy workaround for a ParseTree <= 3.0.7 bug. Methods defined in a
    # "grandparent" module result in an invalid Sexp tree, containing "[nil]"
    # for each such method in the body of the :module node.
    # Upstream bug: http://rubyforge.org/tracker/index.php?func=detail&aid=29095&group_id=439&atid=1778
    raw_ast.delete_if {|n| n == [nil]}

    return Unifier.new.process(raw_ast)
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
      next unless /^__state__.+$/.match def_name.to_s

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

    mod.instance_methods.each do |m|
      rv << m if m =~ /^__bloom__.+$/
    end

    return rv.compact.uniq
  end
end
