module Bud
  ######## methods for registering collection types
  def define_collection(name)
    if @tables.has_key? name
      raise Bud::CompileError, "collection already exists: #{name}"
    end

    # Rule out collection names that use reserved words, including
    # previously-defined method names.
    reserved = eval "defined?(#{name})"
    unless reserved.nil?
      raise Bud::CompileError, "symbol :#{name} reserved, cannot be used as collection name"
    end
    self.singleton_class.send(:define_method, name) do |*args, &blk|
      if blk.nil?
        return @tables[name]
      else
        return @tables[name].pro(&blk)
      end
    end
  end

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
    @tables[name] = (mode ? Bud::BudInputInterface : BudOutputInterface).new(name, self, schema)
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

  def signal(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudSignal.new(name, self, schema)
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
  def file_reader(name, filename, delimiter='\n')
    define_collection(name)
    @tables[name] = Bud::BudFileReader.new(name, filename, delimiter, self)
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
    @tables[name] = Bud::BudTerminal.new(name, [:line], self)
    @channels[name] = @tables[name]
  end
end
