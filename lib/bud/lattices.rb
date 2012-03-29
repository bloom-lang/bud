class Bud::Lattice
  @@lattice_kinds = {}
  # XXX: replace with sets
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

# TODO:
# * pending
# * code blocks (pro)
# * morphism optimization (seminaive)
# * invalidation/rescan?

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
    if @outputs.empty? and @pending.empty?
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
    @outputs
  end

  # Push-based data movement
  def insert(v)
    push_out(v)
  end

  def push_out(v)
    @outputs.each {|o| o.insert(v)}
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

  # Tick (delta processing)
  def tick
  end

  def tick_deltas
  end

  def rescan_at_tick
    false
  end
end

class Bud::LatticeScannerElement < Bud::LatticePushElement
  attr_reader :collection

  def initialize(bud_instance, collection)
    super(bud_instance)
    @collection = collection
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
    if i.class <= Bud::Lattice
      # given a concrete value (enumerable or lattice element), merge the value
      # into the current wrapper delta value
      @new_delta = do_merge(current_new_delta, i)
    elsif i.class <= Bud::LatticeWrapper
      # given a lattice wrapper or a dataflow element, wire up the dataflow to
      # enable push-based evaluation
      add_merge_target
      i.to_push_elem.wire_to self
    elsif i.class <= Bud::LatticePushElement
      add_merge_target
      i.wire_to self
    else
      raise
    end
    self
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
      scanner = Bud::LatticeScannerElement.new(@bud_instance, self)
      toplevel.scanners[this_stratum][[oid, @tabname]] = scanner
      toplevel.push_sources[this_stratum][[oid, @tabname]] = scanner
    end
    toplevel.scanners[this_stratum][[oid, @tabname]]
  end

  def flush_deltas
  end

  superator "<+" do |i|
    if i.class <= Bud::Lattice
      @pending = do_merge(current_pending, i) unless i.nil?
    elsif i.class <= Bud::LatticeWrapper
      add_merge_target
      i.to_push_elem.wire_to(self, :pending)
    elsif i.class <= Bud::LatticePushElement
      add_merge_target
      i.wire_to(self, :pending)
    else
      raise
    end
  end

  # Merge "i" into @new_delta
  def insert(i)
    @new_delta = do_merge(current_new_delta, i)
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
