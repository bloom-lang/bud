module BudState
  ######## methods for registering collection types
  def check_table(name, keys=[], cols=[])
    # rule out tablenames that used reserved words
    reserved = eval "defined?(#{name})"
    unless (reserved.nil? or (reserved == "method" and @tables[name]))
      # first time registering table, check for method name reserved
      raise Bud::BudError, "symbol :#{name} reserved, cannot be used as table name"
    end

    # tick previously-defined tables and tick
    if @tables[name] then
      # check for consistent redefinition, and "tick" the table
      if @tables[name].keys != keys or @tables[name].cols != cols then
        raise Bud::BudError, "create :#{name}, keys = #{keys.inspect}, cols = #{cols.inspect} \n \
        table :#{name} already defined as #{@tables[name].keys.inspect} #{@tables[name].cols.inspect}"
      end
      @tables[name].tick
      return @tables[name]
    else 
      self.singleton_class.send(:define_method, name) do 
        @tables[name]
      end 
      return nil
    end
  end

  def table(name, keys=[], cols=[], conf=nil)
    check_table(name, keys, cols)
    @tables[name] ||= Bud::BudTable.new(name, keys, cols, self, conf)
  end

  def blackboard(name, keys=[], cols=[])
    table(name, keys, cols, "last")
  end

  def permanent(name, keys=[], cols=[])
    table(name, keys, cols, "first")
  end

  def scratch(name, keys=[], cols=[])
    check_table(name, keys, cols)
    @tables[name] ||= Bud::BudScratch.new(name, keys, cols, self)
  end

  def serializer(name, keys=[], cols=[])
    check_table(name, keys, cols)
    @tables[name] ||= Bud::BudSerializer.new(name, keys, cols, self)
  end

  def remove_at(cols)
    i = cols.find_index{ |k| k[0].chr == '@'}
    cols[i] = cols[i].delete('@') unless i.nil?
    return i, cols
  end

  def channel(name, keys=[], cols=[])
    locspec, keys = remove_at(keys)
    locspec, cols = remove_at(cols) if keys.nil?
    raise Bud::BudError, "channel declaration for #{name} missing an address spec" if locspec.nil?
    check_table(name, keys, cols)
    @channels[name] = locspec
    @tables[name] ||= Bud::BudChannel.new(name, keys, cols, locspec, self)
  end

  def file_reader(name, filename, delimiter='\n')
    check_table(name, ['lineno'], ['text'])
    @tables[name] ||= Bud::BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, keys=['ident'], cols=['time'], duration=1)
    if cols.length != 1 or keys.length != 1 then
      raise Bud::BudError("periodic collection #{name} must have one key column, and one other column") 
    end
    t = check_table(name, keys, cols)
    @tables[name] ||= Bud::BudPeriodic.new(name, keys, cols, self)
    unless @periodics.has_key? [name]
      retval = [name, gen_id, duration]
      @periodics << retval
    else
      retval = @periodics.find([name]).first
    end
    return retval
  end

  def terminal(name, cols=['line'])
    raise Bud::BudError("terminal collection #{name} can have only one column") if cols.length != 1
    t = check_table(name, [], cols)
    @tables[name] ||= Bud::BudTerminal.new(name, [], cols, self)
  end

  # methods to define vars and tmpvars.  This code still quite tentative
  def regvar(name, collection)
    # rule out varnames that used reserved words
    reserved = defined?(name)
    if reserved == "method" and not collection[name] then
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