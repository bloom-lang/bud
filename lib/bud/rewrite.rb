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
      :schema => 1, :keys => 1, :values => 1, :cols => 1, :key_cols => 1, 
      :val_cols => 1, :payloads => 1, :~ => 1
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

  def call_is_attr_deref?(recv, op)
    if recv.first == :call and @bud_instance.tables.has_key? recv[2]
      cols = @bud_instance.tables[recv[2]].cols
      return true if cols and cols.include? op
    end
    return false
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
        # for CALM analysis, mark deletion rules as non-monotonic
        @nm = true if op == :-@
        # don't worry about monotone ops, safe lattice morphisms, table names,
        # table.attr calls, or accessors of iterator variables
        unless @monotonic_whitelist[op] or is_lattice_morph(exp) or @bud_instance.tables.has_key? op or call_is_attr_deref?(recv, op) or recv.first == :lvar
          @nm = true
        end
      end
      if @temp_ops[op]
        @temp_op = op.to_s.gsub("@", "")
      end
      super
    end
  end

  def is_lattice_morph(exp)
    recv, op, args = exp
    return false unless recv.sexp_type == :call
    _, lat_name, _ = recv.sexp_body
    lattice = @bud_instance.lattices[lat_name]
    return false if lattice.nil?
    return lattice.class.morphs.has_key? op
  end

  def collect_rhs(exp)
    @collect = true
    rhs = process exp
    @collect = false
    return rhs
  end

  def reset_instance_vars
    @tables = {}
    @nm = false
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

    @rules << [@rule_indx, lhs, op, rule_txt, rule_txt_orig]
    @tables.each_pair do |t, non_monotonic|
      @depends << [@rule_indx, lhs, op, t, non_monotonic]
    end

    reset_instance_vars
    @rule_indx += 1
  end

  def do_table(exp)
    t = exp[1].to_s
    # If we're called on a "table-like" part of the AST that doesn't correspond
    # to an extant collection or lattice, ignore it.
    @tables[t] = @nm if @bud_instance.tables.has_key? t.to_sym or
                        @bud_instance.lattices.has_key? t.to_sym
    drain(exp)
    return t
  end

  def do_rule(exp)
    lhs = process exp[0]
    op = exp[1]
    pro_rules = map2pro(exp[2])
    if @bud_instance.options[:no_attr_rewrite]
      rhs = collect_rhs(pro_rules)
      rhs_pos = rhs
    else
      # need a deep copy of the rules so we can keep a version without AttrName Rewrite
      pro_rules2 = Marshal.load(Marshal.dump(pro_rules))
      rhs = collect_rhs(pro_rules)
      reset_instance_vars
      rhs_pos = collect_rhs(AttrNameRewriter.new(@bud_instance).process(pro_rules2))
    end
    record_rule(lhs, op, rhs_pos, rhs)
    drain(exp)
  end

  # We want to rewrite "map" calls on BudCollections to "pro" calls. It is hard
  # to do this accurately (issue #225), so we just replace map calls liberally
  # and define Enumerable#pro as an alias for "map".
  def map2pro(exp)
    if exp[1] and exp[1][0] and exp[1][0] == :iter \
      and exp[1][1] and exp[1][1][1] and exp[1][1][1][0] == :call
      if exp[1][1][2] == :map
        exp[1][1][2] = :pro
      end
    end
    exp
  end

  def drain(exp)
    exp.shift until exp.empty?
    return ""
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

  # some icky special-case parsing to find mapping between collection names and iter vars
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
      elsif exp[2] and exp[2][0] == :masgn and not @collnames.empty? # join iter
        next unless exp[2][1] and exp[2][1][0] == :array
        @collnames.each_with_index do |c, i|
          next unless exp[2][1][i+1] and exp[2][1][i+1][0] == :lasgn
          @iterhash[exp[2][1][i+1][1]] = c
        end
      end
    end
    (1..(exp.length-1)).each {|i| exp[i] = process(exp[i])}
    exp
  end

  def gather_collection_names(exp)
    if exp[0] == :call and exp[1].nil?
      @collnames << exp[2]
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
    @keyword = :temp

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
        if meth == @keyword and recv.nil?         
          block[i] = rewrite_me(n)
          @did_work = true
        end
      end
    end
    s(tag, name, args, scope)
  end

  def fix_temp_decl(iter_body)
    if iter_body.first.sexp_type == :call
      call_node = iter_body.first

      _, recv, meth, meth_args = call_node
      if meth == @keyword and recv.nil?
        _, lhs, op, rhs = meth_args.sexp_body.first

        old_rhs_body = rhs.sexp_body
        rhs[1] = s(:iter)
        rhs[1] += old_rhs_body
        rhs[1] += iter_body[1..-1]
        return call_node
      end
    end
    return nil
  end

  def get_state_meth(klass)
    return if @tmp_tables.empty?
    block = s(:block)

    @tmp_tables.each do |t|
      args = s(:arglist, s(:lit, t.to_sym))
      block << s(:call, nil, :temp, args)
    end

    meth_name = Module.make_state_meth_name(klass).to_s + "__" + @keyword.to_s
    return s(:defn, meth_name.to_sym, s(:args), s(:scope, block))
  end

  private
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

# We do four things here for each "with" block
# 1) Remove it from the AST
# 2) Use rewrite_me in the parent class to get the collection name pushed onto @tmp_tables.
# 3) Extract the definition of the "with" collection and push it onto @with_defns
# 4) Extract the rules in the body of the "with" block and push it onto @with_rules

class WithExpander < TempExpander
  attr_reader :with_rules, :with_defns
  def initialize
    super()
    @keyword = :with
    @with_rules = []
    @with_defns = []
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
          block[i] = nil
          iter_body = n.sexp_body
          n = fix_temp_decl(iter_body)
          @with_defns.push n
          @did_work = true unless n.nil?
        end

        _, recv, meth, meth_args = n
        if meth == @keyword and recv.nil?
          block[i] = nil
          n = rewrite_me(n)
          @with_defns.push n
          @did_work = true unless n.nil?
        end
      end
    end
    block.compact! unless block.nil? # remove the nils that got pulled out

    return s(tag, name, args, scope)
  end

  def get_state_meth(klass)
    return if @tmp_tables.empty?
    block = s(:block)

    t = @tmp_tables.pop
    args = s(:arglist, s(:lit, t.to_sym))
    block << s(:call, nil, :temp, args)

    meth_name = Module.make_state_meth_name(klass).to_s + "__" + @keyword.to_s
    return s(:defn, meth_name.to_sym, s(:args), s(:scope, block))
  end
  
  private
  def rewrite_me(exp)
    _, recv, meth, args = exp

    raise Bud::CompileError unless recv == nil
    nest_call = args.sexp_body.first
    raise Bud::CompileError unless nest_call.sexp_type == :call

    nest_recv, nest_op, nest_args = nest_call.sexp_body
    raise Bud::CompileError unless nest_recv.sexp_type == :lit

    tmp_name = nest_recv.sexp_body.first
    @tmp_tables.push tmp_name
    nest_block = args.sexp_body[1]
    if nest_block.first == :call
      # a one-rule block doesn't get wrapped in a block.  wrap it ourselves.
      nest_block = s(:block, nest_block)
    end
    @with_rules.push nest_block
    new_recv = s(:call, nil, tmp_name, s(:arglist))
    return s(:call, new_recv, nest_op, nest_args)
  end  
  
  undef get_state_meth
  
  public
  def get_state_meth(klass)
    return if @tmp_tables.empty?
    block = s(:block)

    args = s(:arglist, s(:lit, @tmp_tables.pop.to_sym))
    block << s(:call, nil, :temp, args)

    meth_name = Module.make_state_meth_name(klass).to_s + "__" + @keyword.to_s
    return s(:defn, meth_name.to_sym, s(:args), s(:scope, block))
  end
end

class DefnRenamer < SexpProcessor # :nodoc: all
  def initialize(local_name, rename_tbl)
    super()
    self.require_empty = false
    self.expected = Sexp
    @local_name = local_name
    @rename_tbl = rename_tbl
  end

  def process_defn(exp)
    tag, name, args, scope = exp
    name_s = name.to_s

    if name_s =~ /^__bootstrap__.+$/
      new_name = name_s.sub(/^(__bootstrap__)(.+)$/, "\\1#{@local_name}__\\2")
    elsif name_s =~ /^__state\d+__/
      new_name = name_s.sub(/^(__state\d+__)(.*)$/, "\\1#{@local_name}__\\2")
    elsif name_s =~ /^__bloom__.+$/
      new_name = name_s.sub(/^(__bloom__)(.+)$/, "\\1#{@local_name}__\\2")
    else
      new_name = "#{@local_name}__#{name_s}"
    end

    new_name = new_name.to_sym
    @rename_tbl[name] = new_name

    # Note that we don't bother to recurse further into the AST: we're only
    # interested in top-level :defn nodes.
    s(tag, new_name, args, scope)
  end
end

module ModuleRewriter # :nodoc: all
  # Do the heavy-lifting to import the Bloom module "mod" into the class/module
  # "import_site", bound to "local_name" at the import site. We implement this
  # by converting the imported module into an AST and rewriting the AST like so:
  #
  #   (a) statements in the module that reference sub-modules are rewritten to
  #       reference the mangled name of the submodule
  #   (b) the module name is mangled to include the local bind name and the
  #       import site
  #   (c) instance method names are mangled to include the local bind name
  #   (d) collection names are mangled to include the local bind name
  #   (e) statements in the module are rewritten to reference the mangled names
  #
  # We then convert the rewritten AST back into Ruby source code using Ruby2Ruby
  # and eval() it to define a new module. We return the name of that newly
  # defined module; the caller can then use "include" to load the module into
  # the import site. Note that additional rewrites are needed to ensure that
  # code in the import site that accesses module contents does the right thing;
  # see Bud#rewrite_local_methods.
  
  @@with_id = 0 # upon initialize
  def self.with_id
    @@with_id
  end
  
  def self.incr_with_id
    @@with_id += 1
  end
  
  def self.do_import(import_site, mod, local_name)
    # ast_process_withs modifies its argument as a side-effect 
    # and returns a matching ast.
    # hence we run it before the other rewrites.
    ast = ast_process_withs(mod)
    ast = ast_flatten_nested_refs(ast, mod.bud_import_table)
    ast = ast_process_temps(ast, mod)
    
    ast, new_mod_name = ast_rename_module(ast, import_site, mod, local_name)
    rename_tbl = {}
    ast = ast_rename_methods(ast, local_name, rename_tbl)
    ast = ast_rename_state(ast, local_name, rename_tbl)
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
  # statement to the AST instead. In the long run we should probably adapt the
  # module rewrite system to work with ParseTree > 3.0.7 and get rid of this
  # code, but that will require further changes.
  def self.get_raw_parse_tree(klass)
    pt = RawParseTree.new(false)
    klassname = klass.name
    klassname = klass.to_s if klassname.empty? #("anon_" + Process.pid.to_s + "_" + klass.object_id.to_s) if klassname.empty
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
  
  def self.ast_mangle_with(w,klass)
    r2r = Ruby2Ruby.new
    
    while st = w.get_state_meth(klass)
      # generate the module
      tmpmod = Module.new

      # add a state block to define a temp for the collection name
      state_src = r2r.process(st)        
      tmpmod.module_eval(state_src)

      # add a bloom block
      bloom_blk = s(:defn, :__bloom__rules, s(:args), s(:scope, s(:block)))
      inblk = bloom_blk[3][1]

      # add in the rule that was in the "with" definition
      newdefn = w.with_defns.pop
      inblk << newdefn unless newdefn.nil?

      # add in all the rules from the body of the "with" block
      newrules = w.with_rules.pop
      newrules.each_with_index do |ast, i|
        inblk << ast unless i == 0
      end
      bloom_src = r2r.process(bloom_blk)

      # eval all that Ruby we generated and import new Module into our code
      tmpmod.module_eval(bloom_src)
      modname = "with__"+ModuleRewriter.with_id.to_s
      klass.import tmpmod => modname.to_sym

      ModuleRewriter.incr_with_id
    end
  end
    
  def self.ast_process_withs(mod)
      # strategy to handle withs:
      # 1) run WithExpander#process to delete the "with" blocks and extract their contents
      # 2) get the state and rules mangled appropriately into modules
      # 3) run mod.import on each
      # 4) call self.get_raw_parse_tree on the result to generate an AST

      ast = get_module_ast(mod)
      w = WithExpander.new
      ast = w.process(ast)
      mod_s, name_s, blocks = ast[0], ast[1], ast[2..-1]
      tag, name, args, scope = blocks[0]
      
      self.ast_mangle_with(w,mod) 
      
      retval = Unifier.new.process(self.get_raw_parse_tree(mod))
      return retval
      # return s(mod_s, name_s, *blocks)
  end

  # Rename the given module's name to be a mangle of import site, imported
  # module, and local bind name. We also rename all the instance methods defined
  # in the module to include the local bind name (including the special "state",
  # "bootstrap", and "bloom" methods).
  def self.ast_rename_module(ast, importer, importee, local_name)
    mod_name = ast.sexp_body.first
    raise Bud::BudError if mod_name.to_s != importee.to_s

    # If the importer or importee modules are nested inside an outer module,
    # strip off the outer module name before using for name mangling purposes
    importer_name = Module.get_class_name(importer)
    importee_name = Module.get_class_name(importee)
    new_name = "#{importer_name}__#{importee_name}__#{local_name}"
    ast[1] = new_name.to_sym

    # XXX: it would be nice to return a Module, rather than a string containing
    # the Module's name. Unfortunately, I can't see how to do that.
    return [ast, new_name]
  end

  def self.ast_rename_methods(ast, local_name, rename_tbl)
    DefnRenamer.new(local_name, rename_tbl).process(ast)
  end

  # Mangle the names of all the collections defined in state blocks found in the
  # given module's AST. Returns a table mapping old => new names.
  def self.ast_rename_state(ast, local_name, rename_tbl)
    # Find all the state blocks in the AST
    raise Bud::BudError unless ast.sexp_type == :module

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

    return ast
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
