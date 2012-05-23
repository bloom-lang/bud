require 'set'
require 'bud/executor/elements'

class Bud::Lattice
  @@lattice_kinds = {}
  @@global_morphs = Set.new
  @@global_mfuncs = Set.new

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
    if mfuncs.include?(name) || @@global_mfuncs.include?(name)
      raise Bud::CompileError, "#{name} declared as both monotone and morph"
    end
    @morphs ||= Set.new
    @morphs << name
    @@global_morphs << name
    define_method(name, &block)
  end

  def self.morphs
    @morphs || Set.new
  end

  def self.global_morphs
    @@global_morphs
  end

  def self.monotone(name, &block)
    if morphs.include?(name) || @@global_morphs.include?(name)
      raise Bud::CompileError, "#{name} declared as both monotone and morph"
    end
    @mfuncs ||= Set.new
    @mfuncs << name
    @@global_mfuncs << name
    define_method(name, &block)
  end

  def self.mfuncs
    @mfuncs || Set.new
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

# TODO:
# * morphism optimization (seminaive)
# * merge logic for set-oriented collections
# * invalidation/rescan/non-monotonic stuff?
# * expressions on RHS ("CollExpr")

class Bud::LatticePushElement
  attr_reader :wired_by, :outputs
  attr_accessor :invalidated, :rescan

  def initialize(bud_instance)
    @bud_instance = bud_instance
    @wired_by = []
    @outputs = []
    @pendings = []
    @invalidated = true
    @rescan = true
  end

  # Wiring
  def wire_to(element, kind=:output)
    case kind
    when :output
      @outputs << element
    when :pending
      @pendings << element
    end

    element.wired_by << self
  end

  def check_wiring
    if @outputs.empty? and @pendings.empty?
      raise Bud::Error, "no output specified for #{inspect}"
    end
  end

  def print_wiring(depth=0, accum="")
    puts "#{'  ' * depth}#{accum} #{inspect}"

    [@outputs, @pendings].each do |buf|
      if buf == @outputs
        next_accum = "=> "
      else
        next_accum = "+> "
      end

      buf.each do |o|
        if o.respond_to? :print_wiring
          o.print_wiring(depth + 1, next_accum)
        else
          puts "#{'  ' * (depth + 1)}#{next_accum} #{o.inspect}"
        end
      end
    end
  end

  def inspect
    "#{self.class}:#{self.object_id.to_s(16)}"
  end

  def wirings
    @outputs + @pendings
  end

  def method_missing(meth, *args, &blk)
    if @bud_instance.wiring?
      Bud::PushApplyMethod.new(@bud_instance, self, meth, args, blk)
    else
      super
    end
  end

  # Push-based dataflow
  def insert(v, source)
    push_out(v)
  end

  def push_out(v)
    @outputs.each {|o| o.insert(v, self)}
    @pendings.each {|o| o <+ v}
  end

  def flush
  end

  def stratum_end
  end

  # Rescan and invalidation
  def add_rescan_invalidate(rescan, invalidate)
  end

  def invalidate_at_tick(rescan, invalidate)
  end

  def invalidate_cache
  end

  # Tick (delta processing)
  def tick
  end

  def tick_deltas
  end

  def rescan_at_tick
    false
  end
end

# A push-based dataflow element that scans a lattice wrapper
class Bud::LatticeScanner < Bud::LatticePushElement
  attr_reader :collection, :rescan_set, :invalidate_set

  def initialize(bud_instance, collection)
    super(bud_instance)
    @collection = collection
    @rescan_set = []
    @invalidate_set = []
  end

  def scan(first_iter)
    if first_iter || true # XXX
      push_out(@collection.current_value)
    else
#      push_out(@collection.delta_value)
    end
  end

  def inspect
    "#{super} [#{collection.qualified_tabname}]"
  end
end

class Bud::LatticeWrapper; end

# A push-based dataflow element that applies a method to a lattice value
class Bud::PushApplyMethod < Bud::LatticePushElement
  SOURCE_TYPES = [Bud::LatticeWrapper, Bud::BudCollection,
                  Bud::LatticePushElement, Bud::PushElement]

  def initialize(bud_instance, recv, meth, args, blk)
    super(bud_instance)
    @recv = recv
    @meth = meth
    @blk = blk
    @args = args.dup
    @recv_cache = nil
    @seen_recv = false

    recv.wire_to(self, :output) # XXX: depends on whether method is a morph?
    bud_instance.push_elems[[self.object_id, recv, meth, blk]] = self

    # Arguments that are normal Ruby values are assumed to remain invariant as
    # rule evaluation progresses; hence, we just pass along those values when
    # invoking the function. Arguments that are derived from lattices or
    # collections might change; hence, we need to wire up the push dataflow to
    # have the current values of the function's arguments passed to this node.

    # Map from input node to a list of indexes; the indexes identify the
    # positions in the args array that should be filled with the node's value
    @input_sources = {}
    @waiting_for_input = Set.new
    @args.each_with_index do |a, i|
      if SOURCE_TYPES.any?{|s| a.kind_of? s}
        if a.kind_of? Bud::LatticeWrapper
          a = a.to_push_elem
        end
        a.wire_to(self, :output)
        @input_sources[a] ||= []
        @input_sources[a] << i
        @waiting_for_input << a
        @args[i] = nil          # Substitute actual value before calling method
      end
    end

    @seen_all_inputs = @waiting_for_input.empty?
  end

  def insert(v, source)
    if source == @recv
      @seen_recv = true
      @recv_cache = v           # XXX: what if we get a delta?
    else
      arg_indexes = @input_sources[source]
      raise Bud::Error, "unknown input #{source}" if arg_indexes.nil?
      arg_indexes.each do |i|
        @args[i] = v            # XXX: what if we get a delta?
      end

      unless @seen_all_inputs
        @waiting_for_input.delete(source)
        @seen_all_inputs = @waiting_for_input.empty?
      end
    end

    if @seen_all_inputs && @seen_recv
      res = @recv_cache.send(@meth, *@args, &@blk)
      push_out(res)
    end
  end

  def inspect
    "#{super} [#{@meth}]"
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :wired_by

  def initialize(tabname, klass, bud_i)
    @tabname = tabname
    @klass = klass
    @bud_instance = bud_i
    @wired_by = []
  end

  def qualified_tabname
    @qualified_tabname ||= @bud_instance.toplevel?  ? @tabname : "#{@bud_instance.qualified_name}.#{@tabname}".to_sym
  end

  def invalidate_at_tick
    false
  end

  def current_value
    @storage ||= @klass.new
    @storage
  end

  def current_new_delta
    @new_delta ||= @klass.new
    @new_delta
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

  def setup_wiring(input, kind)
    if input.class <= Bud::LatticeWrapper
      add_merge_target
      input.to_push_elem.wire_to(self, kind)
    elsif (input.class <= Bud::LatticePushElement || input.class <= Bud::PushElement)
      add_merge_target
      input.wire_to(self, kind)
    else
      raise Bud::Error
    end
  end

  public
  def <=(i)
    if @bud_instance.wiring?
      setup_wiring(i, :output)
    else
      @new_delta = do_merge(current_new_delta, i) unless i.nil?
    end
  end

  superator "<+" do |i|
    if @bud_instance.wiring?
      setup_wiring(i, :pending)
    else
      @pending = do_merge(current_pending, i) unless i.nil?
    end
  end

  # XXX: refactor with BudCollection to avoid duplication of code
  def add_merge_target
    toplevel = @bud_instance.toplevel
    if toplevel.done_bootstrap
      toplevel.merge_targets[toplevel.this_stratum] << self
    end
  end

  def to_push_elem
    toplevel = @bud_instance.toplevel
    this_stratum = toplevel.this_stratum
    oid = self.object_id
    unless toplevel.scanners[this_stratum][[oid, @tabname]]
      scanner = Bud::LatticeScanner.new(@bud_instance, self)
      toplevel.scanners[this_stratum][[oid, @tabname]] = scanner
      toplevel.push_sources[this_stratum][[oid, @tabname]] = scanner
    end
    return toplevel.scanners[this_stratum][[oid, @tabname]]
  end

  def flush_deltas
  end

  def add_rescan_invalidate(rescan, invalidate)
  end

  # Merge "i" into @new_delta
  def insert(i, source)
    @new_delta = do_merge(current_new_delta, i)
  end

  def method_missing(meth, *args, &blk)
    # If we're invoking a lattice method and we're currently wiring up the
    # dataflow, wire up a dataflow element to invoke the given method.
    if @bud_instance.wiring?
      pusher = to_push_elem
      Bud::PushApplyMethod.new(@bud_instance, pusher, meth, args, blk)
    else
      super
    end
  end

  def bootstrap
    @storage = do_merge(current_value, @pending)
    @pending = nil
  end

  def tick
    if @new_delta
      raise Bud::Error, "orphaned delta value for lattice #{@tabname}: #{@new_delta.inspect}"
    end
    @storage = do_merge(current_value, @pending)
    @pending = nil
    @delta = nil
  end

  def tick_deltas
    result = false

    if @new_delta
      m = do_merge(current_value, @new_delta)
      if m.reveal != current_value.reveal
        @storage = m
        result = true
      end
    end

    @delta = @new_delta
    @new_delta = nil

    return result
  end

  def inspect
    "{#{@tabname}, #{current_value.inspect}}"
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
