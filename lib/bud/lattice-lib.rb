require 'bud/lattice-core'

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
    # Since bottom of lmax is negative infinity, + is a no-op
    return self if @v.nil?
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
    # Since bottom of lmin is infinity, + is a no-op
    return self if @v.nil?
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

  morph :key_set do
    Bud::SetLattice.new(@v.keys)
  end

  monotone :size do
    Bud::MaxLattice.new(@v.size)
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
  # XXX: probably not the right solution
  def ==(o)
    return false unless o.kind_of? Bud::SetLattice
    return @v.sort == o.reveal.sort
  end

  morph :intersect do |i|
    wrap_unsafe(@v & i.reveal)
  end

  morph :product do |i, &blk|
    rv = []
    @v.each do |a|
      if blk.nil?
        rv += i.reveal.map {|b| [a,b]}
      else
        rv += i.reveal.map{|b| blk.call(a, b)}.reject{|e| e.nil?}
      end
    end

    if blk.nil?
      wrap_unsafe(rv)
    else
      self.class.new(rv)
    end
  end

  morph :contains? do |i|
    Bud::BoolLattice.new(@v.member? i)
  end

  morph :pro do |&blk|
    # Don't use wrap_unsafe(), since the user-supplied code block might
    # introduce duplicates or illegal elements.
    self.class.new(@v.map(&blk).reject{|e| e.nil?})
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

  # Assuming that this hashset contains tuples (arrays), this returns a list of
  # tuples (possibly empty) whose idx'th column has the value "v".
  # XXX: we assume that probe(idx, v) will only be called for a single value of
  # idx!
  def probe(idx, v)
    @ht ||= build_ht(idx)
    return @ht[v] || []
  end

  private
  def build_ht(idx)
    rv = {}
    @v.each do |i|
      field = i[idx]
      rv[field] ||= []
      rv[field] << i
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
