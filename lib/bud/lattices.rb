require 'set'

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

  # The default equality semantics for lattice objects is based on reveal. Note
  # that for certain lattices (e.g., set, map), this might not give the right
  # behavior (e.g., since set's reveal returns an Array).
  def ==(o)
    return false unless o.kind_of? Bud::Lattice
    return reveal == o.reveal
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

  def seal
    Bud::SealedLattice.new(self)
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

  def current_value(&blk)
    @storage ||= @klass.new
    if blk.nil?
      @storage
    else
      @storage.pro(&blk)        # NB: not all lattices implement this method
    end
  end

  # This returns a value for the lattice that is suitable as input to a
  # morphism. On the first iteration, we use the "complete" current value of the
  # lattice; in subsequent iterations, we use the delta value for the lattice
  # (if any).
  def current_morph_value(&blk)
    if @bud_instance.stratum_first_iter
      current_value(&blk)
    else
      current_delta(&blk)
    end
  end

  def current_new_delta
    @new_delta ||= @klass.new
    @new_delta
  end

  private
  def current_delta(&blk)
    @delta ||= @klass.new
    if blk.nil?
      @delta
    else
      @delta.pro(&blk)          # NB: not all lattices implement this method
    end
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
    if rhs.class <= Enumerable && !(rhs.class <= Hash)
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
    @new_delta = do_merge(current_new_delta, i)
  end

  superator "<+" do |i|
    return if i.nil?
    @pending = do_merge(current_pending, i)
  end

  def tick
    if @new_delta
      raise Bud::Error, "orphaned delta value for lattice #{@tabname}: #{@new_delta.inspect}"
    end
    @storage = nil if @is_scratch
    @storage = do_merge(current_value, @pending)
    @pending = nil
    @delta = nil
  end

  def tick_deltas
    result = false

    if @new_delta
      m = do_merge(current_value, @new_delta)
      if m != current_value
        @storage = m
        result = true
      end
    end

    @delta = @new_delta
    @new_delta = nil

    return result
  end

  def inspect
    "lat = #{@tabname}, #{current_value.inspect}"
  end
end

class Bud::MaxLattice < Bud::Lattice
  wrapper_name :lmax

  def initialize(i=nil)
    unless i.nil? || i.class <= Comparable
      reject_input(i)
    end
    @v = i
  end

  def merge(i)
    i_val = i.reveal
    (@v.nil? || (i_val != nil && i_val > @v)) ? i : self
  end

  morph :gt do |k|
    Bud::BoolLattice.new(!!(@v && @v > k))
  end

  morph :gt_eq do |k|
    Bud::BoolLattice.new(!!(@v && @v >= k))
  end

  # XXX: support MaxLattice input?
  morph :+ do |i|
    raise Bud::Error, "cannot apply + to empty MaxLattice"  if @v.nil?
    reject_input(i, "+") unless i.class <= Numeric
    self.class.new(@v + i)
  end

  morph :min_of do |i|
    reject_input(i, "min_of") unless i.class <= Numeric
    (@v.nil? || i < @v) ? self.class.new(i) : self
  end

  def lt_eq(k)
    Bud::BoolLattice.new(!!(@v && @v <= k))
  end
end

class Bud::MinLattice < Bud::Lattice
  wrapper_name :lmin

  def initialize(i=nil)
    unless i.nil? || i.class <= Comparable
      reject_input(i)
    end
    @v = i
  end

  def merge(i)
    i_val = i.reveal
    (@v.nil? || (i_val != nil && i_val < @v)) ? i : self
  end

  morph :lt do |k|
    Bud::BoolLattice.new(!!(@v && @v < k))
  end

  # XXX: support MinLattice input
  morph :+ do |i|
    raise Bud::Error if @v.nil?
    reject_input(i, "+") unless i.class <= Numeric
    self.class.new(@v + i)
  end
end

# XXX: consider creating two fixed ("interned") values for true and false.
class Bud::BoolLattice < Bud::Lattice
  wrapper_name :lbool

  def initialize(i=false)
    reject_input(i) unless [true, false].include? i
    @v = i
  end

  def merge(i)
    self.class.new(@v || i.reveal)
  end

  # XXX: ugly syntax
  morph :when_true do |&blk|
    blk.call if @v
  end
end

class Bud::MapLattice < Bud::Lattice
  wrapper_name :lmap

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
    wrap_unsafe(rv)
  end

  def inspect
    "<#{self.class.wrapper}: #{@v.inspect}>"
  end

  # XXX: If the key is not in the map, we would like to return some generic
  # "bottom" value that is shared by all lattice values. Unfortunately, such a
  # value does not exist, so we need the caller to tell us which class to use as
  # an optional second argument (if omitted, reading a non-existent key produces
  # a runtime exception). Another alternative would be to specify the type of
  # the map's values when the lmap is declared, but that limits code reuse.
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

  morph :key_set do
    Bud::SetLattice.new(@v.keys)
  end

  monotone :size do
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
    wrap_unsafe(rv)
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
  wrapper_name :lset

  # XXX: We take an Enumerable as input. When converting a set-valued expression
  # into a set lattice value, this is a little awkward: because of the "implicit
  # fold" behavior, our input is an array of singleton arrays. It would be a bit
  # nicer to allow the input to be an array of atoms; not clear the best way to
  # achieve that.
  def initialize(i=[])
    reject_input(i) unless i.class <= Enumerable
    i.each do |e|
      reject_input(i) if e.class <= Bud::Lattice
    end
    @v = i.uniq
  end

  def merge(i)
    wrap_unsafe(@v | i.reveal)
  end

  # Override == because we don't keep the underlying array eagerly sorted
  def ==(o)
    return false unless o.kind_of? Bud::SetLattice
    return @v.sort == o.reveal.sort
  end

  morph :intersect do |i|
    wrap_unsafe(@v & i.reveal)
  end

  morph :product do |i|
    rv = []
    @v.each do |a|
      rv += i.pro {|b| [a,b]}
    end
    wrap_unsafe(rv)
  end

  morph :contains? do |i|
    Bud::BoolLattice.new(@v.member? i)
  end

  morph :pro do |&blk|
    @v.map(&blk)
  end

  monotone :size do
    Bud::MaxLattice.new(@v.size)
  end

  # Assuming that this set contains tuples (arrays) as elements, this performs
  # an equijoin between the current lattice and i. The join predicate is
  # "self_t[lhs_idx] == i_t[rhs_idx]", for all tuples self_t and i_t in self and
  # i, respectively. The return value is the result of passing pairs of join
  # tuples to the user-supplied block.
  morph :eqjoin do |i, lhs_idx, rhs_idx, &blk|
    rv = []
    @v.each do |a|
      rv += i.probe(rhs_idx, a[lhs_idx]).map {|b| blk.call(a, b)}
    end
    wrap_unsafe(rv)
  end

  # Assuming that this set contains tuples (arrays) as elements, this returns a
  # list of tuples (possibly) empty whose idx'th column has the value "v".
  def probe(idx, v)
    @ht ||= build_ht(idx)
    return @ht[v] || []
  end

  private
  def build_ht(idx)
    rv = {}
    @v.each do |i|
      rv[i[idx]] ||= []
      rv[i[idx]] << i
    end
    rv
  end
end

# A set that admits only non-negative numbers. This allows "sum" to be an
# order-preserving map.  Note that this does duplicate elimination on its input,
# so it actually computes "SUM(DISTINCT ...)" in SQL.
class Bud::PositiveSetLattice < Bud::SetLattice
  wrapper_name :lpset

  def initialize(i=[])
    super
    @v.each do |n|
      reject_input(i) unless n.class <= Numeric
      reject_input(i) if n < 0
    end
  end

  monotone :pos_sum do
    @sum = @v.reduce(0) {|sum,i| sum + i} if @sum.nil?
    Bud::MaxLattice.new(@sum)
  end
end

# Similar to SetLattice, except that we implement the lattice using a hash table
# rather than an array. This makes merge() much cheaper but incurs somewhat more
# overhead for small sets.
class Bud::HashSetLattice < Bud::Lattice
  wrapper_name :lhset

  def initialize(i=[])
    reject_input(i) unless i.class <= Enumerable
    i.each do |e|
      reject_input(i) if e.class <= Bud::Lattice
    end

    i = Set.new(i) unless i.class <= Set
    @v = i
  end

  def merge(i)
    wrap_unsafe(@v | i.reveal)
  end

  morph :intersect do |i|
    wrap_unsafe(@v & i.reveal)
  end

  morph :product do |i|
    rv = Set.new
    @v.each do |a|
      rv.merge(i.pro {|b| [a,b]})
    end
    wrap_unsafe(rv)
  end

  morph :contains? do |i|
    Bud::BoolLattice.new(@v.member? i)
  end

  morph :pro do |&blk|
    @v.map(&blk)
  end

  monotone :size do
    Bud::MaxLattice.new(@v.size)
  end

  # Assuming that this hashset contains tuples (arrays) as elements, this
  # performs an equijoin between the current lattice and i. The join predicate
  # is "self_t[lhs_idx] == i_t[rhs_idx]", for all tuples self_t and i_t in self
  # and i, respectively. The return value is the result of passing pairs of join
  # tuples to the user-supplied block.
  morph :eqjoin do |i, lhs_idx, rhs_idx, &blk|
    rv = Set.new
    @v.each do |a|
      i.probe(rhs_idx, a[lhs_idx]).each do |b|
        rv << blk.call(a, b)
      end
    end
    wrap_unsafe(rv)
  end

  # Assuming that this hashset contains tuples (arrays) as elements, this
  # returns a list of tuples (possibly) empty whose idx'th column has the value
  # "v".
  def probe(idx, v)
    @ht ||= build_ht(idx)
    return @ht[v] || []
  end

  private
  def build_ht(idx)
    rv = {}
    @v.each do |i|
      rv[i[idx]] ||= []
      rv[i[idx]] << i
    end
    rv
  end
end

# XXX: Should this be just syntax sugar for a map lattice instead?
class Bud::BagLattice < Bud::Lattice
  wrapper_name :lbag

  def initialize(i={})
    reject_input(i) unless i.class <= Hash
    i.each do |k, mult|
      reject_input(i) if k.class <= Bud::Lattice
      reject_input(i) unless (mult.class <= Integer && mult > 0)
    end
    @v = i
  end

  # Note that for merge to be idempotent, we need to use the traditional
  # definition of multiset union (per-element max of multiplicities, rather than
  # sum of multiplicities).
  def merge(i)
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      [lhs_v, rhs_v].max
    end
    wrap_unsafe(rv)
  end

  morph :intersect do |i|
    i_tbl = i.reveal
    # Scan the smaller one, probe the larger one
    scan, probe = (@v.size < i_tbl.size ? [@v, i_tbl] : [i_tbl, @v])
    rv = {}
    scan.each do |k,val|
      rv[k] = [val, probe[k]].min if probe.has_key? k
    end
    wrap_unsafe(rv)
  end

  morph :mult do |k|
    rv = @v[k]
    rv ||= 0
    Bud::MaxLattice.new(rv)
  end

  morph :+ do |i|
    rv = @v.merge(i.reveal) do |k, lhs_v, rhs_v|
      lhs_v + rhs_v
    end
    self.class.new(rv)
  end

  morph :contains? do |i|
    Bud::BoolLattice.new(@v.has_key? i)
  end

  monotone :size do
    Bud::MaxLattice.new(@v.size)
  end
end

# A SealedLattice wraps another lattice value and does not allow that wrapped
# value to change.
class Bud::SealedLattice < Bud::Lattice
  wrapper_name :lseal

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
    return self.class.new(i_val) if @v.nil?

    # If the merge doesn't result in a change to the lattice value, allow it
    m = @v.merge(i_val)
    return self if m == @v

    raise Bud::Error, "cannot merge a sealed lattice value: #{self.inspect}, input = #{i.inspect}"
  end

  # XXX: should this be a monotone func or a morphism?
  monotone :safely do |f, *args|
    # Since this is monotone, we might be placed in the same strata as the rule
    # that defines the sealed value. Hence, the sealed value might initially be
    # nil, but should be defined before the end of this strata, by which time
    # this method *should* have been invoked again. This is a little dubious,
    # but seems hard to avoid.
    @v.send(f, *args) unless @v.nil?
  end
end
