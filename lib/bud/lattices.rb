class Bud::Lattice
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

  def initialize(wrapper, v=nil)
    @wrapper = wrapper
    v = wrap(v) unless v.nil?
    @v = v
  end

  def <=(i)
    return if i.nil?
    @wrapper.merge(i)
  end

  superator "<+" do |i|
    return if i.nil?
    @wrapper.merge_pending(i)
  end

  def reveal
    @v
  end
end

class Bud::LatticeWrapper
  attr_reader :name, :is_scratch
  attr_accessor :got_delta

  def initialize(name, klass, is_scratch)
    @name = name
    @klass = klass
    @is_scratch = is_scratch
    @got_delta = false
    @storage = nil
    @delta = nil
    @pending = nil
  end

  def current_value
    @storage ||= @klass.new(self)
    @storage
  end

  private
  def current_delta
    @delta ||= current_value
    @delta
  end

  def current_pending
    @pending ||= @klass.new(self)
    @pending
  end

  def do_merge(lhs, rhs)
    raise Bud::Error unless lhs.class <= Bud::Lattice

    if rhs.class <= Enumerable
      rhs.each do |r|
        next if r.nil?
        lhs = scalar_merge(lhs, @klass.new(self, r))
      end
      return lhs
    end

    scalar_merge(lhs, rhs)
  end

  def do_scalar_merge(lhs, r)
    rv = lhs.merge(r)
    @got_delta = true if rv.reveal != lhs.reveal
    rv
  end

  public
  def merge(i)
    @delta = do_merge(current_delta, i)
  end

  def merge_pending(i)
    @pending = do_merge(current_pending, i)
  end

  def tick
    @storage = nil if @is_scratch
    @storage = do_merge(current_value, @pending)
    @pending = nil
    raise Bud::Error unless @delta.nil?
  end

  def tick_deltas
    @storage = @delta
    @delta = nil
  end
end

class Bud::MaxLattice < Bud::Lattice
  lattice_name :lat_max

  def wrap(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].max
  end
end

class Bud::MinLattice < Bud::Lattice
  lattice_name :lat_min

  def wrap(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].min
  end
end
