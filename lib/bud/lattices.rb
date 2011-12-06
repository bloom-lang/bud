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

  def self.lat_name
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

  def self.reject_input(i)
    raise Bud::TypeError, "illegal #{self.name} input: #{i.inspect}"
  end

  def initialize(v=nil)
    @v = self.class.convert_to(v)
  end

  # Return the state valued associated with a lattice instance. Note that this
  # is non-monotonic when invoked from user code; it should be used with care by
  # framework code.
  def reveal
    @v
  end

  def inspect
    "<#{self.class.lat_name}: #{reveal}>"
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
    @storage ||= @klass.new
    @storage
  end

  private
  def current_delta
    @delta || current_value
  end

  def current_pending
    @pending ||= @klass.new
    @pending
  end

  def scalar_merge(lhs, rhs)
    unless rhs.class <= @klass
      rhs = @klass.new(rhs)
    end
    rv = lhs.merge(rhs)
    unless rv.class <= Bud::Lattice
      raise Bud::Error, "merge for #{lhs.class} does not return lattice value: #{rv.inspect}"
    end
    rv
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
        lhs = scalar_merge(lhs, r)
      end
      return lhs
    end

    scalar_merge(lhs, rhs)
  end

  public
  def <=(i)
    return if i.nil?
    rv = do_merge(current_delta, i)
    @delta = rv unless rv.reveal == current_delta.reveal
  end

  superator "<+" do |i|
    return if i.nil?
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

  def self.convert_to(i)
    if i.nil? || i.class <= Comparable
      i
    else
      reject_input(i)
    end
  end

  def merge(i)
    (@v.nil? || i.reveal > @v) ? i : self
  end

  morph :gt
  def gt(k)
    Bud::BoolLattice.new(@v && @v > k)
  end
end

class Bud::MinLattice < Bud::Lattice
  lattice_name :lmin

  def self.convert_to(i)
    if i.nil? || i.class <= Comparable
      i
    else
      reject_input(i)
    end
  end

  def merge(i)
    (@v.nil? || i.reveal < @v) ? i : self
  end

  morph :lt
  def lt(k)
    Bud::BoolLattice.new(@v && @v < k)
  end

  morph :+
  def +(i)
    raise Bud::Error unless @v
    Bud::MinLattice.new(@v + i)
  end
end

class Bud::BoolLattice < Bud::Lattice
  lattice_name :lbool

  def self.convert_to(i)
    i ||= false
    if i == true || i == false
      i
    else
      reject_input(i)
    end
  end

  def merge(i)
    Bud::BoolLattice.new(@v || i.reveal)
  end
end

class Bud::MapLattice < Bud::Lattice
  lattice_name :lmap

  def self.convert_to(i)
    i ||= {}
    if i.class == Hash
      i.keys.each do |k|
        reject_input(i) if k.class <= Bud::Lattice
      end
      i.values.each do |v|
        reject_input(i) unless v.class <= Bud::Lattice
      end
      i
    else
      reject_input(i)
    end
  end

  def merge(i)
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      lhs_v.merge(rhs_v)
    end
    Bud::MapLattice.new(rv)
  end
end
