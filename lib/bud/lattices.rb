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

  def self.morph(name, &block)
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

  def reject_input(i, meth="initialize")
    raise Bud::TypeError, "illegal input to #{self.class.lat_name}\##{meth}: #{i.inspect}"
  end

  # Return the state valued associated with a lattice instance. Note that this
  # is non-monotonic when invoked from user code; it should be used with care by
  # framework code.
  def reveal
    @v
  end

  def inspect
    "<#{self.class.lat_name}: #{reveal.inspect}>"
  end

  def seal
    Bud::SealedLattice.new(self)
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :is_scratch

  def initialize(tabname, klass, is_scratch)
    @tabname = tabname
    @klass = klass
    @is_scratch = is_scratch
  end

  def current_value(&blk)
    @storage ||= @klass.new
    if blk.nil?
      @storage
    else
      @storage.pro(&blk)        # NB: not all lattices implement this method
    end
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

  def initialize(i=nil)
    unless i.nil? || i.class <= Comparable
      reject_input(i)
    end
    @v = i
  end

  def merge(i)
    (@v.nil? || i.reveal > @v) ? i : self
  end

  morph :gt do |k|
    Bud::BoolLattice.new(!!(@v && @v > k))
  end

  # XXX: support MaxLattice input?
  morph :+ do |i|
    raise Bud::Error, "cannot apply + to empty MaxLattice"  if @v.nil?
    reject_input(i, "+") unless i.class <= Numeric
    Bud::MaxLattice.new(@v + i)
  end

  morph :min_of do |i|
    reject_input(i, "min_of") unless i.class <= Numeric
    (@v.nil? || i < @v) ? Bud::MaxLattice.new(i) : self
  end

  def lt_eq(k)
    Bud::BoolLattice.new(!!(@v && @v <= k))
  end
end

class Bud::MinLattice < Bud::Lattice
  lattice_name :lmin

  def initialize(i=nil)
    unless i.nil? || i.class <= Comparable
      reject_input(i)
    end
    @v = i
  end

  def merge(i)
    (@v.nil? || i.reveal < @v) ? i : self
  end

  morph :lt do |k|
    Bud::BoolLattice.new(!!(@v && @v < k))
  end

  # XXX: support MinLattice input
  morph :+ do |i|
    raise Bud::Error if @v.nil?
    reject_input(i, "+") unless i.class <= Numeric
    Bud::MinLattice.new(@v + i)
  end
end

# XXX: consider creating two fixed ("interned") values for true and false.
class Bud::BoolLattice < Bud::Lattice
  lattice_name :lbool

  def initialize(i=false)
    reject_input(i) unless [true, false].include? i
    @v = i
  end

  def merge(i)
    Bud::BoolLattice.new(@v || i.reveal)
  end

  # XXX: ugly syntax
  morph :when_true do |&blk|
    blk.call if @v
  end
end

class Bud::MapLattice < Bud::Lattice
  lattice_name :lmap

  def initialize(i={})
    reject_input(i) unless i.class == Hash
    i.keys.each {|k| reject_input(i) if k.class <= Bud::Lattice}
    i.values.each {|v| reject_input(i) unless v.class <= Bud::Lattice}
    @v = i
  end

  def merge(i)
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      lhs_v.merge(rhs_v)
    end
    Bud::MapLattice.new(rv)
  end

  def inspect
    "<#{self.class.lat_name}: #{@v.inspect}>"
  end

  # XXX: If the key is not in the map, we would like to return some generic
  # "bottom" value that is shared by all lattice values. Unfortunately, such a
  # value does not exist, so we need the caller to tell us which value to use if
  # they care. Another alternative is to wire the types of the lattice value
  # into the definition of the map lattice.
  morph :at do |k, *args|
    if @v.has_key? k
      @v[k]
    else
      raise Bud::Error if args.empty?
      default = args.first
      default.new
    end
  end

  morph :key? do |k|
    Bud::BoolLattice.new(@v.has_key? k)
  end

  morph :size do
    Bud::MaxLattice.new(@v.size)
  end

  morph :pro do |&blk|
    @v.map(&blk)
  end

  morph :intersect do |i|
    i_tbl = i.reveal
    # Scan the smaller map, probe the larger one
    scan, probe = (@v.size < i_tbl.size ? [@v, i_tbl] : [i_tbl, @v])
    rv = {}
    scan.each do |k,val|
      rv[k] = val.merge(probe[k]) if probe.has_key? k
    end
    Bud::MapLattice.new(rv)
  end

  # Return true if this map is strictly smaller than or equal to the given
  # map. "x" is strictly smaller than or equal to "y" if:
  #     (a) every key in "x"  also appears in "y"
  #     (b) for every key k in "x", x[k] <= y[k]
  #
  # NB: For this to be a morphism, we require that (a) "self" is deflationary
  # (or fixed) (b) the input lattice value is inflationary (or fixed). We
  # currently don't have a way to express (a) in the type system.
  def lt_eq(i)
    reject_input(i, "lt_eq") unless i.class <= self.class

    @v.each do |k, v|
      unless i.key?(k).reveal == true
        return Bud::BoolLattice.new(false)
      end
      unless v.lt_eq(i.at(k).reveal).reveal == true
        return Bud::BoolLattice.new(false)
      end
    end

    return Bud::BoolLattice.new(true)
  end
end

class Bud::SetLattice < Bud::Lattice
  lattice_name :lset

  # XXX: We take an Enumerable as input. When converting a set-valued expression
  # into a set lattice value, this is a little awkward: because of the "implicit
  # fold" behavior, our input is an array of singleton arrays. It would be a bit
  # nicer to allow the input to be an array of atoms; not clear the best way to
  # achieve that.
  def initialize(i=[])
    reject_input(i) unless i.class <= Enumerable
    i.each do |e|
      # XXX: is there anything wrong with a set lattice that has lattice values
      # as elements?
      reject_input(i) if e.class <= Bud::Lattice
    end
    @v = i
  end

  def merge(i)
    self.class.new(@v | i.reveal)
  end

  morph :intersect do |i|
    self.class.new(@v & i.reveal)
  end

  morph :product do |i|
    rv = []
    @v.each do |a|
      rv += i.pro {|b| [a,b]}
    end
    self.class.new(rv)
  end

  morph :pro do |&blk|
    @v.map(&blk)
  end

  morph :size do
    Bud::MaxLattice.new(@v.size)
  end
end

# XXX: This is a weird bag, because we assume that inputs are unique-ified via
# nonces; hence, merge is the merge we want for the shopping cart example,
# rather than the natural idempotent merge for multisets (max of item
# multiplicities; cart wants sum).
class Bud::BagLattice < Bud::Lattice
  lattice_name :lbag

  def initialize(i={})
    reject_input(i) unless i.class == Hash

    i.each do |k,v|
      reject_input(i) unless k.class <= Bud::SafeNonce

      reject_input(i) unless (v.class <= Enumerable && v.length == 2)
      val, cnt = v
      reject_input(i) if val.class <= Bud::Lattice
      reject_input(i) unless cnt.class <= Numeric
      reject_input(i) if cnt < 0
    end

    @v = i
  end

  def merge(i)
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      raise Bud::Error unless lhs_v == rhs_v
      lhs_v
    end
    return Bud::BagLattice.new(rv)
  end

  # Create an extra lookup table on the fly, mapping each value to its
  # multiplicity. Since any given lattice value is immutable, this table won't
  # change, but we don't bother building it unless needed.
  def make_hash
    rv = {}
    @v.each_value do |t|
      val, cnt = t
      rv[val] ||= 0
      rv[val] += cnt
    end
    rv
  end

  private :make_hash

  morph :key? do |k|
    @h ||= make_hash
    Bud::BoolLattice.new(@h.has_key? k)
  end

  morph :mult do |k|
    @h ||= make_hash
    m = @h[k]
    m ||= 0
    Bud::MaxLattice.new(m)
  end

  morph :size do
    Bud::MaxLattice.new(@v.size)
  end
end

# A set that admits only non-negative numbers. This allows "sum" to be a
# morphism. Note that this does duplicate elimination on its input, so it
# actually computes "SUM(DISTINCT ...)" in SQL.
class Bud::PositiveSetLattice < Bud::SetLattice
  lattice_name :lpset

  def initialize(i=[])
    super
    @v.each do |n|
      reject_input(i) unless n.class <= Numeric
      reject_input(i) if n < 0
    end
  end

  morph :pos_sum do
    @sum = @v.reduce(0) {|sum,i| sum + i} if @sum.nil?
    Bud::MaxLattice.new(@sum)
  end
end

# A SealedLattice wraps another lattice value and does not allow that wrapped
# value to change.
class Bud::SealedLattice < Bud::Lattice
  lattice_name :lseal

  def initialize(i=nil)
    unless i.nil? || i.class <= Bud::Lattice
      reject_input(i)
    end
    @v = i
  end

  def merge(i)
    # If either operand to merge is nil, return the non-nil operand. This
    # strictly violates the "sealed" behavior of the lattice.
    i_val = i.reveal
    return self if i_val.nil?
    return Bud::SealedLattice.new(i_val) if @v.nil?

    # If the merge doesn't result in a change to the lattice value, allow it
    m = @v.merge(i_val)
    return self if m == @v

    raise Bud::Error, "cannot merge a sealed lattice value: #{self.inspect}, input = #{i.inspect}"
  end

  morph :safely do |f, *args|
    # Since this is a morphism, we might be placed in the same strata as the
    # rule that defines the sealed value. Hence, the sealed value might
    # initially be nil, but should be defined before the end of this strata, by
    # which time this method *should* have been invoked again. This is a little
    # dubious, but seems hard to avoid.
    @v.send(f, *args) unless @v.nil?
  end
end
