module BudState
  ######## methods for registering collection types
  def define_collection(name)
    # rule out table names that use reserved words
    reserved = eval "defined?(#{name})"
    unless (reserved.nil? or (reserved == "method" and @tables[name]))
      # first time registering table, check for method name reserved
      raise Bud::BudError, "symbol :#{name} reserved, cannot be used as table name"
    end
    self.singleton_class.send(:define_method, name) do
      @tables[name]
    end
  end
  
  def define_or_tick_collection(name)
    # tick previously-defined tables and tick
    if @tables[name]
      @tables[name].tick
    else
      define_collection(name)
    end
  end

  def input
    true
  end

  def output
    false
  end

  def interface(mode, name, schema=nil)
    @provides[name.to_s] = mode
    scratch(name, schema)
  end

  def table(name, schema=nil)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTable.new(name, self, schema)
  end

  def scratch(name, schema=nil)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudScratch.new(name, self, schema)
  end

  def serializer(name, schema=nil)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudSerializer.new(name, self, schema)
  end

  def channel(name, schema=nil)
    define_or_tick_collection(name)

    unless @tables[name]
      @tables[name] = Bud::BudChannel.new(name, self, schema)
      @channels[name] = @tables[name].locspec_idx
    end
  end

  def file_reader(name, filename, delimiter='\n')
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, period=1)
    define_or_tick_collection(name)
    # stick with default schema -- [:key] => [:val]
    @tables[name] ||= Bud::BudPeriodic.new(name, self)
    unless @periodics.has_key? [name]
      retval = [name, gen_id, period]
      @periodics << retval
    end
  end

  def terminal(name)
    if defined?(@terminal) && @terminal != name
      raise Bud::BudError, "can't register IO collection #{name} in addition to #{@terminal}"
    else
      @terminal = name
    end
    define_or_tick_collection(name)
    @channels[name] = nil
    @tables[name] ||= Bud::BudTerminal.new(name, [:line], self)
  end

  def tctable(name, schema=nil)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTcTable.new(name, self, schema)
    @tc_tables[name] ||= @tables[name]
  end

  def zktable(name, path, addr="localhost:2181")
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudZkTable.new(name, path, addr, self)
    @zk_tables[name] ||= @tables[name]
  end
end
