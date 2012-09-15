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
    i.each_pair do |k,val|
      reject_input(i) if k.class <= Bud::Lattice
      reject_input(i) unless val.class <= Bud::Lattice
    end
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
  # "bottom" value that is shared by all lattice types. Unfortunately, such a
  # value does not exist, so we need the caller to tell us which class to use as
  # an optional second argument (if omitted, fetching a non-existent key yields
  # a runtime exception). Another alternative would be to specify the type of
  # the map's values when the lmap is declared, but that hinders code reuse.
  morph :at do |k, *args|
    if @v.has_key? k
      @v[k]
    else
      raise Bud::Error if args.empty?
      args.first.new
    end
  end

  morph :apply_morph do |sym, *args|
    raise Bud::Error unless Bud::Lattice.global_morphs.include? sym
    do_apply(sym, args)
  end

  monotone :apply_monotone do |sym, *args|
    raise Bud::Error unless Bud::Lattice.global_mfuncs.include? sym
    do_apply(sym, args)
  end

  def do_apply(sym, args)
    rv = {}
    @v.each_pair do |k, val|
      res = val.send(sym, *args)
      raise Bud::Error unless res.kind_of? Bud::Lattice
      rv[k] = res
    end
    wrap_unsafe(rv)
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

  # Produce a Bloom collection (array of tuples) from this lmap, optionally
  # applying a user-provided code block to each (k,v) pair in turn. Note that
  # this is slightly different from how projection over an lmap would work: we
  # return an array, whereas projection would return an lmap.
  morph :to_collection do |&blk|
    @v.map(&blk)
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

# A set lattice contains zero or more primitive (non-lattice) values.
class Bud::SetLattice < Bud::Lattice
  wrapper_name :lset

  def initialize(i=[])
    reject_input(i) if i.any? {|e| e.kind_of? Bud::Lattice}

    i = Set.new(i) unless i.kind_of? Set
    @v = i
  end

  def merge(i)
    wrap_unsafe(@v | i.reveal)
  end

  morph :intersect do |i|
    wrap_unsafe(@v & i.reveal)
  end

  morph :product do |i, &blk|
    rv = Set.new
    @v.each do |a|
      if blk.nil?
        t = i.pro {|b| [a,b]}
      else
        t = i.pro {|b| blk.call(a, b)}
      end
      rv.merge(t.reveal)
    end
    wrap_unsafe(rv)
  end

  morph :contains? do |i|
    Bud::BoolLattice.new(@v.member? i)
  end

  morph :pro do |&blk|
    # We don't use Set#map, since it returns an Array (ugh).
    rv = Set.new
    @v.each do |t|
      val = blk.call(t)
      rv << val unless val.nil?
    end
    wrap_unsafe(rv)
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
    rv = Set.new
    @v.each do |a|
      i.probe(rhs_idx, a[lhs_idx]).each do |b|
        rv << blk.call(a, b)
      end
    end
    wrap_unsafe(rv)
  end

  # Assuming that this set contains tuples (arrays), this returns a list of
  # tuples (possibly empty) whose idx'th column has the value "v".
  # XXX: we assume probe(idx, v) will only be called for a single value of idx!
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

# A set that admits only non-negative numbers. This allows "sum" to be an
# order-preserving map.  Note that this does duplicate elimination on its input,
# so it actually computes "SUM(DISTINCT ...)" in SQL.
#
# XXX: for methods that take a user-provided code block, we need to ensure that
# the set continues to contain only positive numbers.
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
    @sum = @v.reduce(:+) if @sum.nil?
    Bud::MaxLattice.new(@sum)
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

  morph :card do |k|
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
