module BudState
  ######## methods for registering collection types
  def define_collection(name, &block)
    # don't allow duplicate table definitions
    if @tables.has_key? name
      raise Bud::BudError, "collection already exists: #{name}"
    end
    # rule out table names that use reserved words
    reserved = eval "defined?(#{name})"
    unless (reserved.nil? or (reserved == "method" and @tables[name]))
      # first time registering table, check for method name reserved
      raise Bud::BudError, "symbol :#{name} reserved, cannot be used as table name"
    end
    self.singleton_class.send(:define_method, name) do |*args, &blk|
	    unless blk.nil? then
        return @tables[name].pro(&blk)
	    else
        return @tables[name]
      end
    end
  end
  
  def tmp(var, coll)
    self.singleton_class.send(:define_method, var) do |*args, &blk|
      unless blk.nil? then
        return coll.map(&blk)
      else
        return coll
      end
    end
  end
  
  
  def wrap_collection(c)
    return lambda do |&blk|
      unless blk.nil? then
        return c.map(&blk)
      else
        return c
      end
    end
  end
  
  def input
    true
  end

  def output
    false
  end

  def interface(mode, name, schema=nil)
    t_provides << [name.to_s, mode]
    scratch(name, schema)
  end

  def table(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudTable.new(name, self, schema)
  end

  def scratch(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudScratch.new(name, self, schema)
  end

  def channel(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudChannel.new(name, self, schema)
    @channels[name] = @tables[name].locspec_idx
  end

  def file_reader(name, filename, delimiter='\n')
    define_collection(name)
    @tables[name] = Bud::BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, period=1)
    define_collection(name)
    # stick with default schema -- [:key] => [:val]
    @tables[name] = Bud::BudPeriodic.new(name, self)
    raise BudError if @periodics.has_key? [name]
    t = [name, gen_id, period]
    @periodics << t
  end

  def terminal(name)
    if defined?(@terminal) && @terminal != name
      raise Bud::BudError, "can't register IO collection #{name} in addition to #{@terminal}"
    else
      @terminal = name
    end
    define_collection(name)
    @channels[name] = nil
    @tables[name] = Bud::BudTerminal.new(name, [:line], self)
  end

  def tctable(name, schema=nil)
    define_collection(name)
    @tables[name] = Bud::BudTcTable.new(name, self, schema)
    @tc_tables[name] = @tables[name]
  end

  def zktable(name, path, addr="localhost:2181")
    define_collection(name)
    @tables[name] = Bud::BudZkTable.new(name, path, addr, self)
    @zk_tables[name] = @tables[name]
  end
end
