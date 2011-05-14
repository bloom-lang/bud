module Bud
  ######## methods for registering collection types
  private
  def define_collection(name, &block)
    # Don't allow duplicate collection definitions
    if @tables.has_key? name
      raise Bud::CompileError, "collection already exists: #{name}"
    end

    # Rule out collection names that use reserved words, including
    # previously-defined method names.
    reserved = eval "defined?(#{name})"
    unless reserved.nil?
      raise Bud::CompileError, "symbol :#{name} reserved, cannot be used as table name"
    end
    self.singleton_class.send(:define_method, name) do |*args, &blk|
      unless blk.nil? then
        return @tables[name].pro(&blk)
      else
        return @tables[name]
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
    t_provides << [name.to_s, mode]
    scratch(name, schema)
  end

  # declare a persistent collection.  default schema <tt>[:key] => [:val]</tt>
  def table(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudTable.new(name, self, schema)
  end

  # declare a transient collection.  default schema <tt>[:key] => [:val]</tt>
  def scratch(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudScratch.new(name, self, schema)
  end

  # declare a scratch in a bloom statement lhs.  schema inferred from rhs.
  def temp(name)
    define_collection(name)
    # defer schema definition until merge
    @tables[name] = Bud::BudTemp.new(name, self, nil, true)
  end

  # declare a transient network collection.  default schema <tt>[:address, :val] => []</tt>
  def channel(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudChannel.new(name, self, schema)
    @channels[name] = @tables[name].locspec_idx
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
    @tables[name] = Bud::BudPeriodic.new(name, self)
    raise BudError if @periodics.has_key? [name]
    t = [name, gen_id, period]
    @periodics << t
  end

  def terminal(name) # :nodoc: all
    if defined?(@terminal) && @terminal != name
      raise Bud::BudError, "can't register IO collection #{name} in addition to #{@terminal}"
    else
      @terminal = name
    end
    define_collection(name)
    @channels[name] = nil
    @tables[name] = Bud::BudTerminal.new(name, [:line], self)
  end

  # declare a TokyoCabinet table
  def tctable(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudTcTable.new(name, self, schema)
    @tc_tables[name] = @tables[name]
  end
  
  # declare a dbm table
  def dbm_table(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudDbmTable.new(name, self, schema)
    @dbm_tables[name] = @tables[name]
  end

  # declare an Apache ZooKeeper table
  def zktable(name, path, addr="localhost:2181")
    define_collection(name)
    @tables[name] = Bud::BudZkTable.new(name, path, addr, self)
    @zk_tables[name] = @tables[name]
  end
end
