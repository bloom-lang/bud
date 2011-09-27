# XXX: make this a module?
class BudLattice
  attr_reader :tabname
  attr_reader :is_scratch
  attr_reader :got_delta

  @@lattice_kinds = {}

  def self.lattice_name(name)
    if @@lattice_kinds.has_key? name
      raise Bud::CompileError, "Duplicate lattice definition: #{name}"
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

  def initialize(tabname, is_scratch)
    @tabname = tabname
    @is_scratch = is_scratch
    @got_delta = false
    @pending = []
    reset
  end

  def tick_deltas
    @got_delta = false
  end

  def tick
    reset if @is_scratch
    @pending.each do |p|
      self <= p
    end
    @pending.clear
  end

  # XXX: An alternative implementation of the pending buffer would be to use
  # another instance of the lattice itself to "store" the pending values:
  # merging a pending value would merge into the "pending" lattice, and at the
  # end of each timestep we'd merge the pending lattice into the primary lattice
  # and reset the pending lattice.
  superator "<+" do |i|
    @pending << i
  end

  # XXX: This returns a new tree of vector lattices on every invocation. It
  # would be better to construct a single tree on the first call and then
  # memoize it.
  def *(i)
    VectorLattice.wrap(self, i)
  end
end

class VectorLattice < BudLattice
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
        # otherwise, throw a runtime exception. Doing this at runtime is
        # somewhat bogus, but hard to avoid due to Ruby's lack of static typing.
        unless l.class.morphs.has_key? meth_name
          raise Bud::BudTypeError, "\"#{meth_name}\" is not a morphism for #{l.class}"
        end
        r = l.send(meth_name, *args)
      end
      return false unless r
    end

    return true
  end

  def VectorLattice.wrap(a, b)
    r = VectorLattice.new("#{a.tabname}__#{b.tabname}__tmp", true)
    r_v = [a, b]
    r.instance_variable_set('@v', r_v)
    r
  end
end

class MaxLattice < BudLattice
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
      raise Bud::BudTypeError, "Illegal RHS for MaxLattice merge: #{i.class}"
    end
    if input_v and (@v.nil? or input_v > @v)
      @v = input_v
      @got_delta = true
    end
  end

  def reveal
    [[@v]]
  end

  morph :gt_k
  def gt_k(k)
    @v and @v > k
  end
end

class BoolLattice < BudLattice
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
      raise Bud::BudTypeError, "Illegal RHS for BoolLattice merge: #{i.class}"
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

class MultiSetLattice < BudLattice
  lattice_name :lat_ms

  def reset
    @v = {}
  end

  # XXX: we need to ensure this is idempotent.
  # XXX: this will be idempotent if we define merge as taking the max of each
  # element's multiplicities, but then we need another way to do "multiset sum".
  def <=(i)
    return if i.nil?

    if i.class <= MultiSetLattice
      input_v = i.instance_variable_get('@v')
      input_v.each do |key,val|
        @v[key] ||= 0
        @v[key] += val
        @got_delta = true
      end
    elsif i.class <= Enumerable
      i.each do |key|
        @v[key] ||= 0
        @v[key] += 1
        @got_delta = true
      end
    else
      raise Bud::BudTypeError
    end
  end

  morph :to_set
  def to_set
    rv = []
    @v.each do |key,val|
      next if val <= 0
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
