# We monkeypatch Module to add support for Bloom's syntax additions: "state",
# "bloom", and "bootstrap" blocks, plus the "import" statement.
class Module
  # import another module and assign to a qualifier symbol: <tt>import MyModule => :m</tt>
  def import(spec)
    raise Bud::CompileError unless (spec.class <= Hash and spec.length == 1)
    mod, local_name = spec.first
    raise Bud::CompileError unless (mod.class <= Module and local_name.class <= Symbol)

    # Attempting to import a module that has already included the Bud module
    # results in problems (and is a bad idea anyway), so disallow it.
    if mod.included_modules.include? Bud
      raise Bud::CompileError, "cannot import #{mod} because it has already included Bud"
    end

    # To correctly expand qualified references to an imported module, we keep a
    # table with the local bind names of all the modules imported by this
    # module. To handle nested references (a.b.c.d etc.), the import table for
    # module X points to X's own nested import table. If a single module
    # attempts to import multiple sub-modules with the same local name, we merge
    # the import tables of all the modules.
    @bud_import_tbl ||= {}
    prev_tbl = @bud_import_tbl[local_name]
    child_tbl = NestedRefRewriter.build_import_table(mod)
    @bud_import_tbl[local_name] = NestedRefRewriter.merge_import_table(prev_tbl, child_tbl)

    rewritten_mod_name = ModuleRewriter.do_import(self, mod, local_name)
    self.module_eval "include #{rewritten_mod_name}"
  end

  # the block of Bloom collection declarations.  one per module.
  def state(&block)
    meth_name = Module.make_state_meth_name(self)
    define_method(meth_name, &block)
  end

  # a ruby block to be run before timestep 1.  one per module.
  def bootstrap(&block)
    meth_name = "__bootstrap__#{Module.get_class_name(self)}".to_sym
    define_method(meth_name, &block)
  end

  # bloom statements to be registered with Bud runtime.  optional +block_name+ 
  # allows for multiple bloom blocks per module, and overriding
  def bloom(block_name=nil, &block)
    # If no block name was specified, generate a unique name
    if block_name.nil?
      @block_id ||= 0
      block_name = "#{Module.get_class_name(self)}__#{@block_id.to_s}"
      @block_id += 1
    else
      unless block_name.class <= Symbol
        raise Bud::CompileError, "bloom block names must be a symbol: #{block_name}"
      end
    end

    # Note that we don't encode the module name ("self") into the name of the
    # method. This allows named blocks to be overridden (via inheritance or
    # mixin) in the same way as normal Ruby methods.
    meth_name = "__bloom__#{block_name}"

    # Don't allow duplicate named bloom blocks to be defined within a single
    # module; this indicates a likely programmer error.
    if instance_methods(false).include? meth_name
      raise Bud::CompileError, "duplicate named bloom block: '#{block_name}' in #{self}"
    end
    define_method(meth_name.to_sym, &block)
  end

  def bud_import_table() #:nodoc: all
    @bud_import_tbl ||= {}
    @bud_import_tbl
  end

  private
  # Return a string with a version of the class name appropriate for embedding
  # into a method name. Annoyingly, if you define class X nested inside
  # class/module Y, X's class name is the string "Y::X". We don't want to define
  # method names with semicolons in them, so just return "X" instead.
  def self.get_class_name(klass)
    klass.name.split("::").last
  end

  # State method blocks are named using an auto-incrementing counter. This is to
  # ensure that we can rediscover the possible dependencies between these blocks
  # after module import (see Bud#call_state_methods).
  def self.make_state_meth_name(klass)
    @state_meth_id ||= 0
    r = "__state#{@state_meth_id}__#{Module.get_class_name(klass)}".to_sym
    @state_meth_id += 1
    return r
  end
end
