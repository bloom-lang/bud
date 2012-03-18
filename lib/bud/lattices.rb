class Bud::Lattice
  @@lattice_kinds = {}
  @@global_morphs = {}
  @@global_mfuncs = {}

  def self.wrapper_name(name)
    if @wrapper_name
      raise Bud::CompileError, "lattice #{self.class} has multiple names"
    end
    if @@lattice_kinds.has_key? name
      raise Bud::CompileError, "duplicate lattice definition: #{name}"
    end
    @@lattice_kinds[name] = self
    @wrapper_name = name
  end

  def self.lattice_kinds
    @@lattice_kinds
  end

  def self.wrapper
    @wrapper_name
  end

  def self.morph(name, &block)
    if mfuncs.has_key?(name) || @@global_mfuncs.has_key?(name)
      raise Bud::CompileError, "#{name} declared as both monotone and morph"
    end
    @morphs ||= {}
    @morphs[name] = true
    @@global_morphs[name] = true
    define_method(name, &block)
  end

  def self.morphs
    @morphs || {}
  end

  def self.global_morphs
    @@global_morphs
  end

  def self.monotone(name, &block)
    if morphs.has_key?(name) || @@global_morphs.has_key?(name)
      raise Bud::CompileError, "#{name} declared as both monotone and morph"
    end
    @mfuncs ||= {}
    @mfuncs[name] = true
    @@global_mfuncs[name] = true
    define_method(name, &block)
  end

  def self.mfuncs
    @mfuncs || {}
  end

  def self.global_mfuncs
    @@global_mfuncs
  end

  def reject_input(i, meth="initialize")
    site = "#{self.class.wrapper}\##{meth}"
    raise Bud::TypeError, "illegal input to #{site}: #{i.inspect}"
  end

  # Return the state valued associated with a lattice instance. Note that this
  # is non-monotonic when invoked from user code; it should be used with care by
  # framework code.
  def reveal
    @v
  end

  def inspect
    "<#{self.class.wrapper}: #{reveal.inspect}>"
  end

  # Construct a new instance of the current class that wraps "new_v". We assume
  # that new_v is already a legal input value for the class, so we can bypass
  # the class's normal initializer -- this avoids redundant error checks.
  def wrap_unsafe(new_v)
    rv = self.class.new
    rv.instance_variable_set('@v', new_v)
    rv
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :is_scratch

  def initialize(tabname, klass, is_scratch, bud_i)
    @tabname = tabname
    @klass = klass
    @is_scratch = is_scratch
    @bud_instance = bud_i
  end
end
