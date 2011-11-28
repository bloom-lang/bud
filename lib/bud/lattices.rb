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
end

class Bud::LatticeWrapper
  attr_reader :name, :is_scratch
  attr_accessor :got_delta

  def initialize(name, klass, is_scratch)
    @name = name
    @klass = klass
    @is_scratch = is_scratch
    @got_delta = false
    @current_val = nil
  end

  def current_value
    @current_val ||= klass.new
    @current_val
  end

  def tick
  end
end

class Bud::MaxLattice < Bud::Lattice
  lattice_name :lat_max

  def initialize
    @v = nil
  end

  def <=(i)
  end
end
