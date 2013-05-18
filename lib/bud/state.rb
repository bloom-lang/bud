module Bud
  ######## methods for registering collection types
  private
  def check_collection_name(name)
    if @tables.has_key? name or @lattices.has_key? name
      raise Bud::CompileError, "collection already exists: #{name}"
    end

    # Rule out collection names that use reserved words, including
    # previously-defined method names.
    reserved = eval "defined?(#{name})"
    unless reserved.nil?
      raise Bud::CompileError, "symbol :#{name} reserved, cannot be used as collection name"
    end
  end

  def define_collection(name)
    check_collection_name(name)

    self.singleton_class.send(:define_method, name) do |*args, &blk|
      if blk.nil?
        return @tables[name]
      else
        return @tables[name].pro(&blk)
      end
    end
  end

  def define_lattice(name)
    check_collection_name(name)

    self.singleton_class.send(:define_method, name) do |*args, &blk|
      if blk.nil?
        return @lattices[name]
      else
        return @lattices[name].pro(&blk)
      end
    end
  end

  public
  def input # :nodoc: all
    true
  end

  def output # :nodoc: all
    false
  end

  # declare a transient collection to be an input or output interface
  def interface(mode, name, schema=nil)
    define_collection(name)
    t_provides << [name.to_s, mode]
    @tables[name] = (mode ? BudInputInterface : BudOutputInterface).new(name, self, schema)
  end

  # declare an in-memory, non-transient collection.  default schema <tt>[:key] => [:val]</tt>.
  def table(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudTable.new(name, self, schema)
  end

  # declare a collection-generating expression.  default schema <tt>[:key] => [:val]</tt>.
  def coll_expr(name, expr, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudCollExpr.new(name, self, expr, schema)
  end

  # declare a syncronously-flushed persistent collection.  default schema <tt>[:key] => [:val]</tt>.
  def sync(name, storage, schema=nil)
    define_collection(name)
    case storage
    when :dbm
      @tables[name] = Bud::BudDbmTable.new(name, self, schema)
      @dbm_tables[name] = @tables[name]
    else
      raise Bud::Error, "unknown synchronous storage engine #{storage.to_s}"
    end
  end

  def store(name, storage, schema=nil)
    define_collection(name)
    case storage
    when :zookeeper
      # treat "schema" as a hash of options
      options = schema
      raise Bud::Error, "Zookeeper tables require a :path option" if options[:path].nil?
      options[:addr] ||= "localhost:2181"
      @tables[name] = Bud::BudZkTable.new(name, options[:path], options[:addr], self)
      @zk_tables[name] = @tables[name]
    else
      raise Bud::Error, "unknown async storage engine #{storage.to_s}"
    end
  end

  # declare a transient collection.  default schema <tt>[:key] => [:val]</tt>
  def scratch(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudScratch.new(name, self, schema)
  end

  def readonly(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudReadOnly.new(name, self, schema)
  end

  # declare a scratch in a bloom statement lhs.  schema inferred from rhs.
  def temp(name)
    define_collection(name)
    # defer schema definition until merge
    @tables[name] = Bud::BudTemp.new(name, self, nil, true)
  end

  # declare a transient network collection.  default schema <tt>[:address, :val] => []</tt>
  def channel(name, schema=nil, loopback=false)
    define_collection(name)
    @tables[name] = Bud::BudChannel.new(name, self, schema, loopback)
    @channels[name] = @tables[name]
  end

  # declare a transient network collection that delivers facts back to the
  # current Bud instance. This is syntax sugar for a channel that always
  # delivers to the IP/port of the current Bud instance. Default schema
  # <tt>[:key] => [:val]</tt>
  def loopback(name, schema=nil)
    schema ||= {[:key] => [:val]}
    channel(name, schema, true)
  end

  # declare a collection to be read from +filename+.  rhs of statements only
  def file_reader(name, filename)
    define_collection(name)
    @tables[name] = Bud::BudFileReader.new(name, filename, self)
  end

  # declare a collection to be auto-populated every +period+ seconds.  schema <tt>[:key] => [:val]</tt>.
  # rhs of statements only.
  def periodic(name, period=1)
    define_collection(name)
    raise Bud::Error if @periodics.has_key? [name]
    @periodics << [name, period]
    @tables[name] = Bud::BudPeriodic.new(name, self)
  end

  def terminal(name) # :nodoc: all
    if defined?(@terminal) && @terminal != name
      raise Bud::Error, "can't register IO collection #{name} in addition to #{@terminal}"
    else
      @terminal = name
    end
    define_collection(name)
    @tables[name] = Bud::BudTerminal.new(name, self)
    @channels[name] = @tables[name]
  end

  # an alternative approach to declaring interfaces
  def interfaces(direction, collections)
    mode = case direction
      when :input then true
      when :output then false
    else
      raise Bud::CompileError, "unrecognized interface type #{direction}"
    end
    collections.each do |tab|
      t_provides << [tab.to_s, mode]
    end 
  end

  # Define methods to implement the state declarations for every registered kind
  # of lattice.
  def load_lattice_defs
    Bud::Lattice.global_mfuncs.each do |m|
      next if RuleRewriter::MONOTONE_WHITELIST.include? m
      if Bud::BudCollection.instance_methods.include? m.to_s
        puts "monotone method #{m} conflicts with non-monotonic method in BudCollection"
      end
    end

    Bud::Lattice.global_morphs.each do |m|
      next if RuleRewriter::MONOTONE_WHITELIST.include? m
      if Bud::BudCollection.instance_methods.include? m.to_s
        puts "morphism #{m} conflicts with non-monotonic method in BudCollection"
      end
    end

    # Sanity-check lattice definitions
    # XXX: We should do this only once per lattice
    Bud::Lattice.lattice_kinds.each do |wrap_name, klass|
      unless klass.method_defined? :merge
        raise Bud::CompileError, "lattice #{wrap_name} does not define a merge function"
      end

      # If a method is marked as monotone in any lattice, every lattice that
      # declares a method of that name must also mark it as monotone.
      meth_list = klass.instance_methods(false).to_set
      Bud::Lattice.global_mfuncs.each do |m|
        next unless meth_list.include? m.to_s
        unless klass.mfuncs.include? m
          raise Bud::CompileError, "method #{m} in #{wrap_name} must be monotone"
        end
      end

      # Apply a similar check for morphs
      Bud::Lattice.global_morphs.each do |m|
        next unless meth_list.include? m.to_s
        unless klass.morphs.include? m
          raise Bud::CompileError, "method #{m} in #{wrap_name} must be a morph"
        end
      end

      # Similarly, check for non-monotone lattice methods that are found in the
      # builtin list of monotone operators. The "merge" method is implicitly
      # monotone (XXX: should it be declared as a morph or monotone function?)
      meth_list.each do |m_str|
        m = m_str.to_sym
        next unless RuleRewriter::MONOTONE_WHITELIST.include? m
        # XXX: ugly hack. We want to allow lattice class implementations to
        # define their own equality semantics.
        next if m == :==
        unless klass.mfuncs.include?(m) || klass.morphs.include?(m) || m == :merge
          raise Bud::CompileError, "method #{m} in #{wrap_name} must be monotone"
        end
      end

      # XXX: replace "self" with toplevel?
      self.singleton_class.send(:define_method, wrap_name) do |lat_name|
        define_lattice(lat_name)
        @lattices[lat_name] = Bud::LatticeWrapper.new(lat_name, klass, self)
      end
    end
  end
end
