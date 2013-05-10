require 'bud/executor/elements'

class Bud::Lattice
  include Comparable

  @@lattice_kinds = {}
  @@global_morphs = Set.new
  @@global_mfuncs = Set.new

  def self.wrapper_name(name)
    if @wrapper_name
      raise Bud::CompileError, "lattice #{self.name} has multiple wrapper names"
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

  # The default equality semantics for lattice objects is based on reveal. Note
  # that this isn't always appropriate: if the intended equality semantics for
  # the lattice type differ from the equality semantics of the object returned
  # by reveal (e.g., a set lattice might return an array with an unpredictable
  # order), the lattice type should override this behavior.
  def ==(o)
    return false unless o.kind_of? Bud::Lattice
    return reveal == o.reveal
  end

  def eql?(o)
    self == o
  end

  # Ensure hashing and equality semantics are consistent.
  def hash
    reveal.hash
  end

  # Similarly, use reveal'ed value to implement Comparable.
  def <=>(o)
    reveal <=> o.reveal
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

class Bud::LatticePushElement
  attr_reader :wired_by, :outputs
  attr_accessor :invalidated, :rescan

  def initialize(bud_instance)
    @bud_instance = bud_instance
    @wired_by = []
    @outputs = []
    @pendings = []
    @deletes = []
    @invalidated = true
    @rescan = true
  end

  def wire_to(element, kind=:output)
    case kind
    when :output
      @outputs << element
    when :pending
      @pendings << element
    when :delete
      @deletes << element
    else
      raise Bud::Error, "unrecognized wiring kind: #{kind}"
    end

    element.wired_by << self
  end

  def check_wiring
    if @outputs.empty? and @pendings.empty? and @deletes.empty?
      raise Bud::Error, "no output specified for #{inspect}"
    end
  end

  def print_wiring(depth=0, accum="")
    puts "#{'  ' * depth}#{accum} #{inspect}"

    [@outputs, @pendings, @deletes].each do |buf|
      next_accum = case buf
                   when @outputs
                     "=> "
                   when @pendings
                     "+> "
                   when @deletes
                     "-> "
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
    @outputs + @pendings + @deletes
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
    @outputs.each do |o|
      # If we're emitting outputs to a traditional Bloom collection, merge
      # operators (e.g., <=, <+) take a collection of tuples, so we need to
      # convert the lattice value into a collection of tuple-like values. For
      # now, we hardcode a single way to do this: we simply assume the value
      # embedded inside the lattice is an Enumerable that contains tuple-like
      # values. We also allow lattice morphisms to just produce Enumerable
      # values directly, so we don't call reveal in that case.
      # XXX: rethink this.
      if o.class <= Bud::BudCollection
        o <= (v.class <= Bud::Lattice ? v.reveal : v)
      else
        o.insert(v, self)
      end
    end
    @pendings.each do |o|
      if o.class <= Bud::BudCollection
        o.pending_merge(v.class <= Bud::Lattice ? v.reveal : v)
      else
        o <+ v
      end
    end
    @deletes.each do |o|
      raise Bud::Error unless o.class <= Bud::BudCollection
      o.pending_delete(v.class <= Bud::Lattice ? v.reveal : v)
    end
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
    if first_iter || @bud_instance.options[:disable_lattice_semi_naive]
      push_out(@collection.current_value)
    else
      push_out(@collection.current_delta)
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
    @is_morph = Bud::Lattice.global_morphs.include? @meth
    @recv_is_scanner = @recv.kind_of? Bud::LatticeScanner

    recv.wire_to(self, :output)
    bud_instance.push_elems[[self.object_id, recv, meth, blk]] = self

    # Arguments that are normal Ruby values are assumed to remain invariant as
    # rule evaluation progresses; hence, we just pass along those values when
    # invoking the function. Arguments that are derived from lattices or
    # collections might change; hence, we need to wire up the push dataflow to
    # have the current values of the function's arguments passed to this node.

    # Map from input node to a list of indexes; the indexes identify the
    # positions in the args array that should be filled with the node's value
    @input_sources = {}

    # Similarly, map from input node to a cached value -- this is the last value
    # we've seen from this input. If the input gave us a delta, we merge
    # together all the deltas we've seen and cache the resulting value.  XXX: In
    # the common case that the input is a scanner over a lattice wrapper, this
    # means we do redundant work merging together deltas.
    @input_caches = {}

    # Inputs for which we haven't seen a value yet.
    @waiting_for_input = Set.new
    @recv_cache = nil
    @seen_recv = false

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
      if @seen_recv
        # Update the cached value for the method receiver. Note that if we're
        # applying a method directly to a LatticeScanner (i.e., method applied
        # to lattice wrapper), we can avoid maintaining a separate cache and
        # instead use the wrapper's current value.
        if @recv_is_scanner
          @recv_cache = @recv.collection.current_value
        else
          @recv_cache = @recv_cache.merge(v)
        end
      else
        @recv_cache = v
      end
      @seen_recv = true
      if @seen_all_inputs
        if @is_morph
          recv_val = v
        else
          recv_val = @recv_cache
        end
        res = recv_val.send(@meth, *@args, &@blk)
        push_out(res)
      end
    else
      arg_indexes = @input_sources[source]
      raise Bud::Error, "unknown input #{source}" if arg_indexes.nil?
      arg_val = v
      unless @is_morph
        if @input_caches[source]
          arg_val = @input_caches[source].merge(arg_val)
        end
      end
      arg_indexes.each do |i|
        @args[i] = arg_val
      end

      unless @seen_all_inputs
        @waiting_for_input.delete(source)
        @seen_all_inputs = @waiting_for_input.empty?
      end

      if @seen_all_inputs && @seen_recv
        res = @recv_cache.send(@meth, *@args, &@blk)
        push_out(res)
      end

      if @input_caches.has_key? source
        @input_caches[source] = @input_caches[source].merge(v)
      else
        @input_caches[source] = v
      end
      arg_indexes.each do |i|
        @args[i] = @input_caches[source]
      end
    end
  end

  def inspect
    "#{super} [#{@meth}]"
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :wired_by, :rescan_on_delta
  attr_accessor :accumulate_tick_deltas, :bud_instance

  def initialize(tabname, klass, bud_i)
    @tabname = tabname
    @klass = klass
    @bud_instance = bud_i
    @wired_by = []
    @rescan_on_delta = Set.new
  end

  def qualified_tabname
    @qualified_tabname ||= @bud_instance.toplevel?  ? @tabname : "#{@bud_instance.qualified_name}.#{@tabname}".to_sym
  end

  def invalidate_at_tick
    false
  end

  def setup_wiring(input, kind)
    if input.class <= Bud::LatticeWrapper
      input.to_push_elem.wire_to(self, kind)
    elsif (input.class <= Bud::LatticePushElement || input.class <= Bud::PushElement)
      input.wire_to(self, kind)
    elsif input.class <= Bud::BudCollection
      input.pro.wire_to(self, kind)
    elsif input.class <= Proc
      tbl = register_coll_expr(input)
      tbl.pro.wire_to(self, kind)
    else
      raise Bud::Error, "unrecognized wiring input: #{input}"
    end

    add_merge_target
  end

  def positive_predecessors
    @wired_by.select {|e| e.outputs.include?(self) || e.pendings.include?(self)}
  end

  private
  def register_coll_expr(expr)
    name = "expr_#{expr.object_id}".to_sym
    @bud_instance.coll_expr(name, expr, nil)
    @bud_instance.send(name)
  end

  public
  def current_value
    @storage ||= @klass.new
    @storage
  end

  def current_delta
    @delta ||= @klass.new
    @delta
  end

  def current_new_delta
    @new_delta ||= @klass.new
    @new_delta
  end

  def current_pending
    @pending ||= @klass.new
    @pending
  end

  def do_merge(lhs, rhs)
    unless lhs.class <= Bud::Lattice
      raise Bud::Error, "unexpected merge input: #{lhs.class}"
    end
    return lhs if rhs.nil?

    unless rhs.class <= @klass
      rhs = @klass.new(rhs)
    end
    rv = lhs.merge(rhs)
    unless rv.class <= Bud::Lattice
      raise Bud::Error, "#{lhs.class}\#merge did not return lattice value: #{rv.inspect}"
    end
    rv
  end

  # Merge "i" into @new_delta
  public
  def insert(i, source)
    @new_delta = do_merge(current_new_delta, i)
  end

  def <=(i)
    if @bud_instance.wiring?
      setup_wiring(i, :output)
    else
      @new_delta = do_merge(current_new_delta, i)
    end
  end

  superator "<+" do |i|
    if @bud_instance.wiring?
      setup_wiring(i, :pending)
    else
      @pending = do_merge(current_pending, i)
    end
  end

  superator "<~" do |o|
    # Overridden when <~ is defined (i.e., channels and terminals)
    raise Bud::CompileError, "#{tabname} cannot appear on the lhs of a <~ operator"
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
    # Bootstrap blocks might install lattice values via either <= (@new_delta)
    # or <+ (@pending).
    if @new_delta
      merge_to_storage(@new_delta)
      @new_delta = nil
    end

    if @pending
      merge_to_storage(@pending)
      @pending = nil
    end
  end

  def tick
    if @new_delta
      raise Bud::Error, "orphaned delta value for lattice #{@tabname}: #{@new_delta.inspect}"
    end
    merge_to_storage(@pending)
    @pending = nil
    @delta = nil
  end

  def merge_to_storage(v)
    m = do_merge(current_value, v)
    if m != current_value
      @storage = m
      @rescan_on_delta.each do |e|
        if e.kind_of? Bud::ScannerElement
          e.force_rescan = true
        else
          e.rescan = true
        end
      end
      return true
    else
      return false
    end
  end

  def tick_deltas
    result = merge_to_storage(@new_delta)
    @delta = @new_delta
    @new_delta = nil
    return result
  end

  def inspect
    "{#{@tabname}, #{current_value.inspect}}"
  end
end
