class Bud::Lattice
end

class Bud::LatticeWrapper
  attr_reader :tabname, :is_scratch

  def initialize(tabname, klass, is_scratch, bud_i)
    @tabname = tabname
    @klass = klass
    @is_scratch = is_scratch
    @bud_instance = bud_i
  end
end
