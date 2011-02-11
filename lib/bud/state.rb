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

  def interface(mode, name, schema)
    @provides[name.to_s] = mode
    scratch(name, schema)
  end

  def table(name, schema)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTable.new(name, schema, self)
  end

  def scratch(name, schema)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudScratch.new(name, schema, self)
  end

  def serializer(name, schema)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudSerializer.new(name, schema, self)
  end

  def channel(name, schema)
    define_or_tick_collection(name)

    unless @tables[name]
      @tables[name] = Bud::BudChannel.new(name, schema, self)
      @channels[name] = @tables[name].locspec
    end
  end

  def file_reader(name, filename, delimiter='\n')
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, period=1)
    define_or_tick_collection(name)
    schema = {[:ident] => [:time]}
    @tables[name] ||= Bud::BudPeriodic.new(name, schema, self)
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

  def tctable(name, schema)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTcTable.new(name, schema, self)
    @tc_tables[name] ||= @tables[name]
  end

  def zktable(name, path, addr="localhost:2181")
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudZkTable.new(name, path, addr, self)
    @zk_tables[name] ||= @tables[name]
  end

  # methods to define vars and tmpvars.  This code still quite tentative
  def regvar(name, collection)
    # rule out varnames that used reserved words
    reserved = defined?(name)
    if reserved == "method" and not collection[name]
      # first time registering var, check for method name reserved
      raise Bud::BudError, "symbol :#{name} reserved, cannot be used as variable name"
    end
    self.singleton_class.send :define_method, name do
      collection[name]
    end
    setter = (name.to_s + '=').to_sym
    self.class.send :define_method, setter do |val|
      curval = collection[name]
      raise Bud::BudError, "#{name} is frozen with value #{curval}" unless curval.nil?
      collection.delete(val)
      collection << [name,val]
      # collection <- [name]
      # collection <+ [name,val]
    end
  end

  def var(name)
    regvar(name, @vars)
  end

  def tmpvar(name)
    regvar(name, @tmpvars)
  end
end
