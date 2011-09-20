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
    reset
  end

  def tick_deltas
    @got_delta = false
  end

  def *(i)
    VectorLattice.wrap(self, i)
  end
end

class VectorLattice < BudLattice
  lattice_name :lat_vec

  def reset
    # XXX: not necessary?
    @v.each {|i| i.reset} if @v
    @v = []
  end

  def <=(i)
    if i.class <= VectorLattice
      input_v = i.instance_variable_get('@v')
      input_v.each_with_index do |l, idx|
        puts "#{tabname}: <= with from #{i.tabname}"
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
    meth_name = args.shift

    @v.each do |l|
      # XXX: check that "meth_name" for l is a morphism
      puts "Invoking #{meth_name} with args = #{args.inspect} on #{l.tabname}"
      r = l.send(meth_name, *args)
      return nil unless r
    end

    return [[true]]
  end

  def VectorLattice.wrap(a, b)
    r = VectorLattice.new("#{a.tabname}__#{b.tabname}__tmp", true)
    puts "VectorLattice.wrap: r = #{r.tabname}"
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

  # XXX: should this return a BoolLattice instance?
  morph :gt_k
  def gt_k(k)
    yield if @v and @v > k
  end
end

class BoolLattice < BudLattice
  lattice_name :lat_bool

  def reset
    @v = false
  end

  def <=(i)
    return if @v
    if i.class <= BoolLattice
      input_v = i.instance_variable_get('@v')
    elsif i.class <= Enumerable
      input_v = false
      i.each do |t|
        next if t.nil? or t == []
        raise BudTypeError unless (t.class <= TrueClass or t.class <= FalseClass)
        if t == true
          input_v = true
          break
        end
      end
    else
      raise BudTypeError, "Illegal RHS for BoolLattice merge: #{o.class}"
    end

    if input_v == true
      @v = input_v
      @got_delta = true
    end
  end

  def reveal
    [[@v]]
  end

  morph :when_true
  def when_true
    yield if @v
  end
end
