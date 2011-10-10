# XXX: make this a module?
class BasicLattice < Bud::BudLattice
  attr_reader :is_scratch
  attr_reader :got_delta

  @@lattice_kinds = {}

  def self.lattice_name(name)
    if @@lattice_kinds.has_key? name
      raise Bud::CompileError, "duplicate lattice definition: #{name}"
    end
    @@lattice_kinds[name] = self
  end

  def self.lattice_kinds
    @@lattice_kinds
  end

  def self.morphs
    @morphs || {}
  end

  def self.morph(name)
    @morphs ||= {}
    @morphs[name] = true
  end

  def initialize(tabname, bud_instance, is_scratch)
    super(tabname, bud_instance)
    @is_scratch = is_scratch
    @got_delta = false
    @pending = nil
    reset
  end

  def tick_deltas
    @got_delta = false
  end

  def tick
    reset if @is_scratch
    if @pending
      self <= @pending
      @pending.reset
    end
  end

  superator "<+" do |i|
    @pending ||= self.class.new("#{tabname}__pending", @bud_instance, true)
    @pending <= i
  end

  # XXX: This returns a new tree of vector lattices on every invocation. It
  # would be better to construct a single tree on the first call and then
  # memoize it.
  def *(i)
    VectorLattice.wrap(self, i, @bud_instance)
  end
end

class VectorLattice < BasicLattice
  lattice_name :lat_vec

  def reset
    @v.each {|i| i.reset} if @v     # XXX: not necessary?
    @v = []
  end

  def <=(i)
    if i.class <= VectorLattice
      input_v = i.instance_variable_get('@v')
      input_v.each_with_index do |l, idx|
        if @v[idx]
          @v[idx] <= l
          @got_delta ||= @v[idx].got_delta
        else
          @v[idx] = l
          @got_delta = true
        end
      end
    else
      raise Bud::BudTypeError
    end
  end

  morph :all?
  def all?(*args)
    return false if @v.empty?
    meth_name = args.shift

    @v.each do |l|
      # Recurse for embedded vectors
      if l.class <= VectorLattice
        r = l.all?(meth_name, *args)
      else
        # Check that "meth_name" is a morphism for this element of the vector;
        # Doing this at runtime is somewhat bogus but hard to avoid due to
        # Ruby's lack of static typing.
        unless l.class.morphs.has_key? meth_name
          raise Bud::BudTypeError, "\"#{meth_name}\" is not a morphism for #{l.class}"
        end
        r = l.send(meth_name, *args)
      end
      return false unless r
    end

    return true
  end

  def VectorLattice.wrap(a, b, bud_instance)
    r = VectorLattice.new("#{a.tabname}__#{b.tabname}__tmp", bud_instance, true)
    r_v = [a, b]
    r.instance_variable_set('@v', r_v)
    r
  end
end

class MaxLattice < BasicLattice
  lattice_name :lat_max

  def reset
    @v = nil
  end

  # XXX: We currently update @v in-place, and set @got_delta whenever @v is
  # changed. stratum_fixpoint() will then call tick_deltas(), which merely
  # resets the @got_delta flag. Instead, we could defer changing @v until
  # tick_deltas(), which would be closer to the normal collection
  # behavior. Which is right?
  def <=(i)
    return if i.nil?
    if i.class <= MaxLattice
      input_v = i.instance_variable_get('@v')
    elsif i.class <= Enumerable
      input_v = nil
      i.each do |t|
        next if t.nil? or t == []
        if input_v.nil? or t[0] > input_v
          input_v = t[0]
        end
      end
    else
      raise Bud::BudTypeError, "illegal RHS for MaxLattice merge: #{i.class}"
    end
    if input_v and (@v.nil? or input_v > @v)
      @v = input_v
      @got_delta = true
    end
  end

  def reveal
    @v
  end

  morph :gt_k
  def gt_k(k)
    @v and @v > k
  end

  def MaxLattice.wrap(val, bud_instance)
    r = MaxLattice.new("tmp_#{rand.to_s[0,6]}", bud_instance, true)
    r.instance_variable_set('@v', val)
    r
  end
end

class BoolLattice < BasicLattice
  lattice_name :lat_bool

  def reset
    @v = false
  end

  def <=(i)
    return if @v or i.nil?
    if i.class <= BoolLattice
      input_v = i.instance_variable_get('@v')
    elsif i.class <= Enumerable
      input_v = false
      i.each do |t|
        next if t.nil? or t == []
        raise Bud::BudTypeError unless (t.class <= TrueClass or t.class <= FalseClass)
        if t == true
          input_v = true
          break
        end
      end
    elsif (i.class <= TrueClass or i.class <= FalseClass)
      input_v = i
    else
      raise Bud::BudTypeError, "illegal RHS for BoolLattice merge: #{i.class}"
    end

    if input_v == true
      @v = input_v
      @got_delta = true
    end
  end

  def reveal
    @v
  end

  morph :to_set
  def to_set
    return [] unless @v
    if block_given?
      yield
    else
      [[true]]
    end
  end
end

class MultiSetLattice < BasicLattice
  lattice_name :lat_ms

  def reset
    @v = {}
  end

  def <=(i)
    return if i.nil?

    if i.class <= MultiSetLattice
      input_v = i.instance_variable_get('@v')
      input_v.each do |key,val|
        raise Bud::BudError if val <= 0
        if @v.has_key?(key) == false or @v[key] < val
          @v[key] = val
          @got_delta = true
        end
      end
    elsif i.class <= Enumerable
      i.each do |t|
        raise Bud::BudError if t.length != 2
        key, val = t
        if @v.has_key?(key) == false or @v[key] < val
          @v[key] = val
          @got_delta = true
        end
      end
    else
      raise Bud::BudTypeError
    end
  end

  # By default, we produce a set containing the elements of the multiset and
  # omit their multiplicities. That is inconsistent with <= with Enumerable
  # input; perhaps this should be fixed.
  # XXX: Returning cardinalities of elements is unsafe, and would make this not
  # a morphism.
  morph :to_set
  def to_set
    rv = []
    @v.each do |key,val|
      if block_given?
        rv << (yield [key, val])
      else
        rv << key
      end
    end
    return rv
  end

  def reveal
    @v
  end
end

class MergeMapLattice < BasicLattice
  lattice_name :lat_map

  def reset
    @v = {}
  end

  private
  def merge_item(key, val)
    if @v.has_key?(key) == false
      @v[key] = val
      @got_delta = true
    else
      @v[key] <= val
      @got_delta ||= @v[key].got_delta
    end
  end

  # XXX: inefficient
  public
  def tick_deltas
    super
    # XXX: potential bug. Suppose a lattice value is inserted as a MergeMap
    # key. In a subsequent timestep, the lattice value is updated via some other
    # means, but the dataflow that connects the lattice to the MergeMap has
    # subsequently been disconnected (e.g., because a join predicate now fails).
    @v.each_value do |val|
      val.tick_deltas
    end
  end

  public
  def <=(i)
    if i.class <= MergeMapLattice
      input_v = i.instance_variable_get('@v')
      input_v.each do |key,val|
        merge_item(key, val)
      end
    elsif i.class <= Enumerable
      i.each do |t|
        raise Bud::BudError if t.length != 2
        key, val = t
        raise Bud::BudError unless val.class <= Bud::BudLattice
        merge_item(key, val)
      end
    end
  end

  morph :all?
  def all?(*args)
    return false if @v.empty?
    meth_name = args.shift

    # XXX: recurse into nested lattices?
    @v.each_value do |val|
      unless val.class.morphs.has_key? meth_name
        raise Bud::BudTypeError
      end
      r = val.send(meth_name, *args)
      return false unless r
    end

    return true
  end

  morph :[]
  def [](k)
    @v[k]
  end

  morph :keys
  def keys
    return @v.keys unless block_given?

    rv = []
    @v.each_key do |key|
      rv << (yield key)
    end
    rv
  end

  morph :to_set
  def to_set
    return @v.to_a unless block_given?

    rv = []
    @v.each do |key,val|
      rv << (yield [key, val])
    end
    rv
  end
end

# TODO
class CountingSetLattice < BasicLattice
  lattice_name :lat_cs
end
