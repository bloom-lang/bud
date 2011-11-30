class Bud::Lattice
  @@lattice_kinds = {}
  @@global_morphs = {}

  def self.lattice_name(name)
    if @lattice_name
      raise Bud::CompileError, "lattice #{self.class} has multiple names"
    end
    if @@lattice_kinds.has_key? name
      raise Bud::CompileError, "duplicate lattice definition: #{name}"
    end
    @@lattice_kinds[name] = self
    @lattice_name = name
  end

  def self.lattice_kinds
    @@lattice_kinds
  end

  def self.name
    @lattice_name
  end

  def self.morph(name)
    @morphs ||= {}
    @morphs[name] = true
    @@global_morphs[name] = true
  end

  def self.morphs
    @morphs || {}
  end

  def self.global_morphs
    @@global_morphs
  end

  def initialize(wrapper, v=nil)
    @wrapper = wrapper
    @v = self.class.convert(v)
  end

  def <=(i)
    @wrapper.merge(i) unless i.nil?
  end

  superator "<+" do |i|
    @wrapper.merge_pending(i) unless i.nil?
  end

  # Return the state valued associated with a lattice instance. Note that this
  # is non-monotonic when invoked from user code; it should be used with care by
  # framework code.
  def reveal
    @v
  end

  def inspect
    "<#{self.class.name}: #{reveal}>"
  end

  # Marshal this lattice value. We don't want to marshal the lattice wrapper (if
  # any), so temporarily remove it from the object (XXX: hack).
  def marshal
    old_wrapper = @wrapper
    begin
      @wrapper = nil
      return Marshal.dump(self)
    ensure
      @wrapper = old_wrapper
    end
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :is_scratch

  def initialize(tabname, klass, is_scratch)
    @tabname = tabname
    @klass = klass
    @is_scratch = is_scratch
  end

  def current_value
    @storage ||= @klass.new(self)
    @storage
  end

  private
  def current_delta
    @delta || current_value
  end

  def current_pending
    @pending ||= @klass.new(self)
    @pending
  end

  def coerce_value(v)
    if v.class <= @klass
      v
    else
      @klass.new(self, v)
    end
  end

  def do_merge(lhs, rhs)
    raise Bud::Error unless lhs.class <= Bud::Lattice
    return lhs if rhs.nil?

    # NB: we assume that all lattices are content with the default set =>
    # lattice homomorphism: we convert each element of the set into a lattice
    # value, and then fold over those lattice values using the merge function.
    if rhs.class <= Enumerable
      rhs.each do |r|
        next if r.nil?
        lhs = scalar_merge(lhs, coerce_value(r))
      end
      return lhs
    end

    scalar_merge(lhs, rhs)
  end

  def scalar_merge(lhs, r)
    coerce_value(lhs.merge(r))
  end

  public
  def merge(i)
    rv = do_merge(current_delta, i)
    @delta = rv unless rv.reveal == current_delta.reveal
  end

  def merge_pending(i)
    @pending = do_merge(current_pending, i)
  end

  def got_delta?
    not @delta.nil?
  end

  def tick
    @storage = nil if @is_scratch
    @storage = do_merge(current_value, @pending)
    @pending = nil
    if @delta
      raise Bud::Error, "orphaned delta value for lattice #{@tabname}: #{@delta.inspect}"
    end
  end

  def tick_deltas
    unless @delta.nil?
      @storage = @delta
      @delta = nil
    end
  end

  def inspect
    "lat = #{@tabname}, #{current_value.inspect}"
  end
end

class Bud::MaxLattice < Bud::Lattice
  lattice_name :lmax

  def self.convert(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].safe_max
  end

  morph :gt
  def gt(k)
    @v and @v > k
  end
end

class Bud::MinLattice < Bud::Lattice
  lattice_name :lmin

  def self.convert(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].safe_min
  end

  morph :lt
  def lt(k)
    @v and @v < k
  end
end

class Bud::BoolLattice < Bud::Lattice
  lattice_name :lbool

  def self.convert(i)
    if i.nil?
      false
    else
      i
    end
  end

  def merge(i)
    @v || i
  end
end
