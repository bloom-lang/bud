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
    @storage ||= klass.new(self)
    @storage
  end

  def current_delta
    @delta ||= current_value
    @delta
  end

  def current_pending
    @pending ||= klass.new(self)
    @pending
  end

  def merge(i)
    prev_delta = current_delta
    @delta = prev_delta.merge(i)
  end

  def merge_pending(i)
    prev_pending = current_pending
    @pending = prev_pending.merge(i)
  end

  def tick
    if @is_scratch
      @storage = nil
    end
    @storage = current_value.merge(@pending)
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

  def merge(i)
    [@v, i.reveal].max
  end
end
