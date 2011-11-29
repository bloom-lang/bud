class Bud::Lattice
  @@lattice_kinds = {}

  def self.lattice_name(name)
    if @@lattice_kinds.has_key? name
      raise Bud::CompileError, "duplicate lattice definition: #{name}"
    end
    @@lattice_kinds[name] = self
    @lattice_name = name
  end

  def self.lattice_kinds
    @@lattice_kinds
  end

  def self.name
    @lattice_name
  end

  def initialize(wrapper, v=nil)
    @wrapper = wrapper
    @v = self.class.convert(v)
  end

  def <=(i)
    @wrapper.merge(i) unless i.nil?
  end

  superator "<+" do |i|
    @wrapper.merge_pending(i) unless i.nil?
  end

  # Return the state valued associated with a lattice instance. Note that this
  # is non-monotonic when invoked from user code; it should be used with care by
  # framework code.
  def reveal
    @v
  end

  def inspect
    "<#{self.class.name}: #{reveal}>"
  end
end

class Bud::LatticeWrapper
  attr_reader :tabname, :is_scratch
  attr_accessor :got_delta

  def initialize(tabname, klass, is_scratch)
    @tabname = tabname
    @klass = klass
    @is_scratch = is_scratch
    @got_delta = false
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
    return if rhs.nil?

    puts "do_merge: lhs = #{lhs.inspect}, rhs = #{rhs.inspect}"

    # NB: we assume that all lattices are content with the default set =>
    # lattice homomorphism: we convert each element of the set into a lattice
    # value, and then fold over those lattice values using the merge function.
    if rhs.class <= Enumerable
      rhs.each do |r|
        next if r.nil?
        lhs = scalar_merge(lhs, @klass.new(self, r))
      end
      return lhs
    end

    scalar_merge(lhs, rhs)
  end

  def scalar_merge(lhs, r)
    # NB: inefficient
    rv = @klass.new(self, lhs.merge(r))
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
    raise Bud::Error, "orphaned delta tuples: #{@delta.inspect}" unless @delta.nil?
  end

  def tick_deltas
    @storage = @delta unless @delta.nil?
    @delta = nil
  end
end

class Bud::MaxLattice < Bud::Lattice
  lattice_name :lmax

  def self.convert(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].safe_max
  end

  def gt(k)
    @v and @v > k
  end
end

class Bud::MinLattice < Bud::Lattice
  lattice_name :lmin

  def self.convert(i)
    if i.class <= Enumerable
      i.first
    else
      i
    end
  end

  def merge(i)
    [@v, i.reveal].safe_min
  end

  def lt(k)
    @v and @v < k
  end
end

class Bud::BoolLattice < Bud::Lattice
  lattice_name :lbool

  def self.convert(i)
    if i.nil?
      false
    else
      i
    end
  end

  def merge(i)
    @v || i
  end
end
