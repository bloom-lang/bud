class BudLattice
  attr_reader :tabname
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

  def initialize(tabname)
    @tabname = tabname
    @got_delta = false
  end

  def tick_deltas
    @got_delta = false
  end
end

class MaxLattice < BudLattice
  lattice_name :lat_max

  def initialize(name)
    super(name)
    @v = nil
  end

  def reveal
    [[@v]]
  end

  # XXX: We currently update @v in-place, and set @got_delta whenever @v is
  # changed. stratum_fixpoint() will then call tick_deltas(), which merely
  # resets the @got_delta flag. Instead, we could defer changing @v until
  # tick_deltas(), which would be closer to the normal collection
  # behavior. Which is right?
  def <=(i)
    if i.class <= MaxLattice
      input_v = i.instance_variable_get('@v')
    elsif i.class <= Enumerable
      first_cols = i.map {|t| t[0]}
      input_v = first_cols.max
    else
      raise BudTypeError, "Illegal RHS for MaxLattice merge: #{o.class}"
    end
    if @v.nil? or input_v > @v
      @v = input_v
      @got_delta = true
    end
  end

  morph :gt_k
  def gt_k(k, &blk)
    if @v and @v > k
      return blk.call
    end
  end
end
