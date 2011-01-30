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
    return nil
  end
  
  def define_or_tick_collection(name)
    # tick previously-defined tables and tick
    if @tables[name]
      @tables[name].tick
      return @tables[name]
    else
      return define_collection(name)
    end
  end

  def input
    true
  end

  def output
    false
  end

  def interface(mode, name, keys, cols=[])
    @provides[name.to_s] = mode
    scratch(name, keys, cols)
  end

  def table(name, keys, cols=[])
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTable.new(name, keys, cols, self)
  end

  def scratch(name, keys, cols=[])
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudScratch.new(name, keys, cols, self)
  end

  def serializer(name, keys, cols=[])
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudSerializer.new(name, keys, cols, self)
  end

  def remove_at(cols)
    i = cols.find_index{ |k| k[0].chr == '@'}
    cols[i] = cols[i].delete('@') unless i.nil?
    return i, cols
  end

  def channel(name, keys, cols=[])
    if @locspec.nil?
      @locspec, keys = remove_at(keys)
      @locspec, cols = remove_at(cols) if keys.nil?
    end
    define_or_tick_collection(name)
    @channels[name] ||= @locspec
    @tables[name] ||= Bud::BudChannel.new(name, keys, cols, @locspec, self)
  end

  def file_reader(name, filename, delimiter='\n')
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, period=1, keys=['ident'], cols=['time'])
    @name = name
    if cols.length != 1 or keys.length != 1
      raise Bud::BudError("periodic collection #{name} must have one key column, and one other column")
    end
    t = define_or_tick_collection(name)
    @tables[name] ||= Bud::BudPeriodic.new(name, keys, cols, self)
    unless @periodics.has_key? [name]
      retval = [name, gen_id, period]
      @periodics << retval
    else
      retval = @periodics.find([name]).first
    end
    return retval
  end

  def terminal(name, keys=['line'])
    if defined?(@terminal) && @terminal != name
      raise Bud::BudError, "can't register IO collection #{name} in addition to #{@terminal}"
    else
      @terminal = name
    end
    raise Bud::BudError("IO collection #{name} can have only one column") if keys.length != 1
    t = define_or_tick_collection(name)
    @channels[name] = nil
    @tables[name] ||= Bud::BudTerminal.new(name, keys, [], self)
  end

  def tctable(name, keys, cols)
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudTcTable.new(name, keys, cols, self)
    @disk_tables[name] ||= @tables[name]
  end

  def zktable(name, path, addr="localhost:2181")
    define_or_tick_collection(name)
    @tables[name] ||= Bud::BudZkTable.new(name, path, addr, self)
    # XXX: refactor
    @disk_tables[name] ||= @tables[name]
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
