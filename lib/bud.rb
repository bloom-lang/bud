require 'enumerator'
require 'msgpack'
require 'eventmachine'
require 'socket'
require 'superators'
require 'bud/aggs'

class Object
  # http://whytheluckystiff.net/articles/seeingMetaclassesClearly.html
  # The hidden singleton lurks behind everyone
  def metaclass; class << self; self; end; end
  def meta_eval &blk; metaclass.instance_eval &blk; end

  # Adds methods to a metaclass
  def meta_def name, &blk
    meta_eval { define_method name, &blk }
  end

  # Defines an instance method within a class
  def class_def name, &blk
    class_eval { define_method name, &blk }
  end
end

class Bud
  attr_reader :strata, :budtime
  attr_accessor :connections
  attr_reader :tables # for debugging; remove me later

  def initialize(ip, port)
    @tables = {}
    @strata = []
    @channels = {}
    @budtime = 0
    @ip = ip
    @port = port.to_i
    $connections = {} if $connections.nil?
    $inbound = [] if $inbound.nil?

    @periodics = table :periodics, ['name'], ['ident', 'duration']
    @vars = table :vars, ['name'], ['value']
    @tmpvars = scratch :tmpvars, ['name'], ['value']
  end  


  ######## methods for controlling execution
  def run
    begin 
      EventMachine::run {
        EventMachine::start_server(@ip, @port, Server) { |con|
          con.bud = self # pass this Bud object into the connection
        }
        puts "running bud server on #{@ip}, #{@port}"
        tick
      }
    end
  end

  def tick
    @budtime += 1
    # reset any schema stuff that isn't already there
    # state to be defined by the user program
    state

    receive_inbound

    # load the rules as a closure (will contain persistent tuples and new inbounds)
    # declaration to be provided by user program
    declaration

    @strata.each { |strat| stratum_fixpoint(strat) }
    @channels.each { |c| @tables[c[0]].flush }
    reset_periodics
  end

  # handle any inbound tuples off the wire and then clear
  def receive_inbound
    $inbound.each do |msg|
      tables[msg[0].to_sym] << msg[1]
    end
    $inbound = []
  end

  def stratum_fixpoint(strat)
    cnts = Hash.new
    @tables.each_key do |k| 
      #      define_method(k.to_sym) { @tables[k] }
      eval "#{k.to_s} = @tables[#{k}]"
    end
    begin
      cnts = {}
      @tables.each_key{|k| cnts[k] = @tables[k].length}
      strat.call
    end while cnts.inject(0){|sum,t| sum + (@tables[t[0]].length - t[1])} > 0
  end

  def reset_periodics
    @periodics.each do |p| 
      if @tables[p.name].length > 0 then
        set_timer(p.name, p.ident, p.duration) 
        @tables[p.name] = scratch p.name, ['ident'], ['time']
      end
    end
  end


  ######## methods for registering collection types
  def table(name, keys=[], cols=[], persist = :table)
    # rule out tablenames that used reserved words
    reserved = defined?(name)
    if reserved == "method" and not @tables[name] then
      # first time registering table, check for method name reserved
      raise BudError, "symbol :#{name} reserved, cannot be used as table name"
    end

    # check for previously-defined tables
    if @tables[name] then
      # check for consistent redefinition, and "tick" the table
      if @tables[name].keys != keys or @tables[name].cols != cols then
        raise BudError, "create :#{name}, keys = #{keys.inspect}, cols = #{cols.inspect} \n \
        table :#{name} already defined as #{@tables[name].keys.inspect} #{@tables[name].cols.inspect}"
      end
      @tables[name].tick
    else # define table
      @tables[name] = case persist
        when :table then BudTable.new(name, keys, cols, self)
        when :channel then BudChannel.new(name, keys, cols, self)
        when :periodic then BudPeriodic.new(name, keys, cols, self)
        when :scratch then BudScratch.new(name, keys, cols, self)
        else raise BudError, "unknown persistence model"
      end
      self.class.send :define_method, name do 
        @tables[name]
      end 
    end
    return @tables[name]
  end

  def scratch(name, keys=[], cols=[])
    table(name, keys, cols, :scratch)
  end

  def channel(name, locspec, keys=[], cols=[])
    t = table(name, keys, cols, :channel)
    t.locspec = locspec
    @channels[name] = locspec
  end

  def periodic(name, duration)
    table(name, ['ident'], ['time'], :periodic)
    unless @periodics.has_key? [name]
      retval = [name, gen_id, duration]
      @periodics << retval
      set_timer(name, retval[1], duration)
    else
      retval = @periodics.find([name]).first
    end
    return retval
  end

  ## methods to define vars and tmpvars.  This code still quite tentative
  def regvar(name, collection)
    # rule out varnames that used reserved words
    reserved = defined?(name)
    if reserved == "method" and not collection[name] then
      # first time registering var, check for method name reserved
      raise BudError, "symbol :#{name} reserved, cannot be used as variable name"
    end
    self.class.send :define_method, name do 
      collection[name]
    end
    setter = (name.to_s + '=').to_sym
    self.class.send :define_method, setter do |val|
      curval = collection[name]
      raise BudError, "#{name} is frozen with value #{curval}" unless curval.nil?
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

  ####### Joins
  def join(rels, *preds)
    # decompose each pred into a binary pred
    newpreds = []
    preds.each do |p|
      p.each_with_index do |c, i|
        newpreds << [p[i], p[i+1]] unless p[i+1].nil?
      end
    end
    BudJoin.new(rels, newpreds)
  end
  
  def natjoin(rels)
    # for all pairs of relations, add predicates on matching column names
    preds = []
    rels.each do |r|
      rels.each do |s|
        matches = r.schema & s.schema
        matches.each do |c| 
          preds << [self.send(r.name).send(c), self.send(s.name).send(c)] unless r == s
        end
      end
    end
    preds.uniq!
    join(rels, *preds)
  end
  
  ######## ids and timers
  def gen_id
    Time.new.to_i.to_s << rand.to_s
  end

  def set_timer(name, id, secs)
    EventMachine::Timer.new(secs) do
      @tables[name] <+ [[id, Time.new.to_s]]
      tick
    end
  end

  ######## the collection types
  class BudCollection
    include Enumerable

    attr_accessor :schema, :keys, :cols
    attr_reader :name, :bud_instance

    def initialize(name, keys, cols, b_class)
      @name = name
      @schema = keys+cols
      @keys = keys
      @storage = {}
      @pending = {}
      @bud_instance = b_class
      raise BudError, "schema contains duplicate names" if schema.uniq.length < schema.length
      schema_accessors
    end

    def clone
      retval = BudCollection.new(keys, schema - keys, bud_instance)
      retval.storage = @storage.clone
      retval.pending = @pending.clone
      return retval
    end   

    def cols
      schema - keys
    end

    def tick
      @storage = @pending
      @pending = {}
    end

    # define methods to turn 'table.col' into a [table,col] pair
    # e.g. to support somethin like 
    #    j = join link, path, {link.to => path.from}
    def schema_accessors
      s = @schema
      m = Module.new do
        s.each_with_index do |c, i|
          define_method c.to_sym do
            [@name, i, c]
          end
        end
      end
      self.extend m  
    end

    # define methods to access tuple attributes by column name
    def tuple_accessors(t)
      s = @schema
      m = Module.new do
        s.each_with_index do |c, i|
          define_method c.to_sym do 
            t[i]
          end
        end
      end
      t.extend m
      #      return t
    end

    def each
      @storage.each_key do |k|
        tup = (@storage[k] == true) ? k : (k + @storage[k])
        yield tuple_accessors(tup)
      end
    end

    def each_pending
      @pending.each_key do |k|
        k = tuple_accessors(k)
        if @pending[k] == true
          yield k
        else
          yield k + @pending[k]
        end
      end
    end

    def do_insert(o, store)
      return if o.nil? or o.length == 0
      keycols = keys.map{|k| o[schema.index(k)]}
      vals = (schema - keys).map{|v| o[schema.index(v)]}
      vals = true if vals.empty?
      if not store[keycols].nil? then
        raise KeyConstraintError, "Key conflict inserting [#{keycols}][#{vals}]" unless store[keycols].nil? or vals == store[keycols]
      end
      store[keycols] = vals unless store[keycols]
      return o
    end

    def insert(o)
      do_insert(o,@storage)
    end

    alias << insert

    def pending_insert(o)
      do_insert(o, @pending)
    end

    def merge(o)
      delta = o.map {|i| self.insert(i)}
      if self.schema.empty? and o.respond_to?(:schema) and not o.schema.empty? then 
        self.schema = o.schema 
      end
      return delta
    end

    alias <= merge

    def pending_merge(o)
      delta = o.map {|i| self.pending_insert(i)}
      if self.schema.empty? and o.respond_to?(:schema) and not o.schema.empty? then 
        self.schema = o.schema 
      end
      return delta
    end

    superator "<+" do |o|
      pending_merge o
    end

    def [](key)
      return nil unless @storage.include? key
      tup = key
      tup += @storage[key] unless @storage[key] == true
      return tuple_accessors(tup)
    end
    
    def method_missing(sym, *args, &block)
      @storage.send sym, *args, &block
    end
    
    ######## aggs

    def argagg(aggname, gbkeys, col)
      agg = bud_instance.send(aggname, nil)[0]
      raise BudError, "#{aggname} not declared exemplary" unless agg.class <= Bud::Exemplary
      keynames = gbkeys.map {|k| k[2]}
      colnum = col[1]
      retval = BudScratch.new('temp', keynames, @schema - keynames, bud_instance)
      tups = self.inject({}) do |memo,p| 
        pkeys = keynames.map{|n| p.send(n.to_sym)}
        if memo[pkeys].nil? then
          memo[pkeys] = [agg.send(:init, p[colnum]), p]
        else
          newval = agg.send(:trans, memo[pkeys][0], p[colnum])
          memo[pkeys] = [newval, p] unless memo[pkeys][0] == newval
        end
        memo
      end
            
      retval.merge(tups.map{|t| agg.send(:final, t[1][1])})      
    end

    def group(keys, *aggpairs)    
      keynames = keys.map {|k| k[2]}
      retval = BudScratch.new('temp', keynames, @schema - keynames, bud_instance)
      tups = self.inject({}) do |memo,p| 
        pkeys = keynames.map{|n| p.send(n.to_sym)}
        memo[pkeys] = [] if memo[pkeys].nil?
        aggpairs.each_with_index do |ap, i|
          agg = ap[0]
          colnum = ap[1].nil? ? nil : ap[1][1]
          colval = colnum.nil? ? nil : p[colnum]
          if memo[pkeys][i].nil? then
            memo[pkeys][i] = agg.send(:init, colval)
          else
            memo[pkeys][i] = agg.send(:trans, memo[pkeys][i], colval)
          end
        end
        memo
      end

      result = tups.inject([]) do |memo,t| 
        finals = []
        aggpairs.each_with_index do |ap, i|
          finals << ap[0].send(:final, t[1][i])
        end
        memo << t[0] + finals
      end
      retval.merge(result)
    end
        
    alias reduce inject
  end

  class BudScratch < BudCollection
  end

  class BudChannel < BudCollection
    attr_accessor :locspec

    def split_locspec(l)
      lsplit = l.split(':')
      lsplit[1] = lsplit[1].to_i
      return lsplit
    end

    def establish_connection(l)
      $connections[l] = EventMachine::connect l[0], l[1], Server
      # rescue
      #   puts "connection #{l} failed"
    end

    def flush
      ip = Bud::instance_variable_get('@ip')
      port = Bud::instance_variable_get('@port')
      each_pending do |t|
        locspec = split_locspec(t[@locspec])
        # remote channel tuples are sent and removed
        if locspec != [ip, port] then
          establish_connection(locspec) if $connections[locspec].nil?
          $connections[locspec].send_data [@name, t].to_msgpack
          @pending.delete t
        end
      end
    end
  end

  class BudPeriodic < BudCollection
  end

  class BudTable < BudCollection
    def initialize(name, keys, cols, bud_instance)
      super(name, keys, cols, bud_instance)
      @to_delete = {}
    end

    def clone
      retval = super
      retval.to_delete = @to_delete.clone
    end

    def tick
      @to_delete.each_key {|t| @storage.delete t}
      @storage.merge! @pending
      @to_delete = {}
      @pending = {}
    end

    superator "<-" do |o|
      # delta = 
      o.map {|i| self.do_insert(i, @to_delete)}
    end
  end

  class BudJoin < BudCollection
    attr_accessor :rels, :origrels

    def initialize(rellist, preds=nil)
      @schema = []
      otherpreds = nil
      @origrels = rellist

      # extract predicates on rellist[0] and let the rest recurse
      unless preds.nil?
        @localpreds = preds.reject { |p| p[0][0] != rellist[0].name and p[1][0] != rellist[0].name }
        @localpreds.each do |p| 
          if p[1][0] == rellist[0].name then
            @localpreds.delete(p)
            @localpreds << [p[1], p[0]]
          end
        end    
        otherpreds = preds.reject { |p| p[0][0] == rellist[0].name or p[1][0] == rellist[0].name}
        otherpreds = nil if otherpreds.empty?
      end
      if rellist.length == 2 and not otherpreds.nil?
        raise BudError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
      end

      # recurse to form a tree of binary BudJoins
      @rels = [rellist[0]]
      @rels << (rellist.length == 2 ? rellist[1] : BudJoin.new(rellist[1..rellist.length-1], otherpreds))

      # now derive schema: combo of rels[0] and rels[1]
      if @rels[0].schema.empty? or @rels[1].schema.empty? then
        @schema = []
      else
        dups = @rels[0].schema & @rels[1].schema
        bothschema = @rels[0].schema + @rels[1].schema
        @schema = bothschema.to_enum(:each_with_index).map  {|c,i| if dups.include?(c) then c + '_' + i.to_s else c end }
      end
    end

    def do_insert(o,store)
      raise BudError, "no insertion into joins"
    end

    def each(&block)
      if @localpreds.nil? then        
        nestloop_join(&block)
      else
        hash_join(&block)
      end
    end

    def test_locals(r, s, *skips)
      retval = true
      if (@localpreds and skips and @localpreds.length > skips.length) then           
        # check remainder of the predicates
        @localpreds.each do |pred|
          next if skips.include? pred
          r_offset, s_index, s_offset = join_offsets(pred)
          if r[r_offset] != s[s_index][s_offset] then
            retval = false 
            break
          end
        end
      end
      return retval
    end

    def nestloop_join(&block)
      @rels[0].each do |r|
        @rels[1].each do |s|
          s = [s] if origrels.length == 2
          yield([r] + s) if test_locals(r, s)
        end  
      end
    end

    def join_offsets(pred)
      build_entry = pred[1]
      build_name, build_offset = build_entry[0], build_entry[1]
      probe_entry = pred[0]
      probe_name, probe_offset = probe_entry[0], probe_entry[1]

      # determine which subtuple of s contains the table referenced in RHS of pred
      # note that s doesn't contain the first entry in rels, which is r      
      index = 0
      origrels[1..origrels.length].each_with_index do |t,i|
        if t.name == pred[1][0] then
          index = i
          break
        end
      end
      
      return probe_offset, index, build_offset
    end

    def hash_join(&block)
      # hash join on first predicate!
      ht = {}

      probe_offset, build_tup, build_offset = join_offsets(@localpreds.first)

      # build the hashtable on s!
      rels[1].each do |s|
        s = [s] if origrels.length == 2
        attrval = s[build_tup][build_offset]
        ht[attrval] ||= []
        ht[attrval] << s
      end

      # probe the hashtable!
      rels[0].each do |r|
        next if ht[r[probe_offset]].nil?
        ht[r[probe_offset]].each do |s|
          retval = [r] + s
          yield([r] + s) if test_locals(r, s, @localpreds.first)
        end
      end
    end
  end
  ######## error types  
  class BudError < Exception
  end

  class KeyConstraintError < BudError
  end

  ######## the EventMachine server for handling network and timers
  class Server < EM::Connection
    attr_accessor :bud

    def initialize(*args)
      @pac = MessagePack::Unpacker.new
      super
    rescue Exception
      print "An error occurred initializing BudServer: ",$!, "\n"
    end

    def post_init
      @port, @ip = Socket.unpack_sockaddr_in(get_peername)
      puts "-- server inbound connection from #{@ip}:#{@port}"
      $connections = {} if $connections.nil?
      $connections[[@ip, @port]] = self
    rescue Exception
      print "An error occurred post_init on BudServer: ",$!, "\n"
    end

    def receive_data(data)
      # Feed the received data to the deserializer
      @pac.feed data

      # streaming deserialize
      @pac.each do |obj|
        message_received(obj)
      end
    end

    def message_received(obj)
      puts "got " + obj.inspect
      if (obj.class <= Array and obj.length == 2 and not bud.tables[obj[0].to_sym].nil? and obj[1].class <= Array) then
        $inbound << obj
        bud.tick
      else
        puts " ... BAD!"
        bud.tick
      end
    end

    def unbind
      puts "-- connection ended from #{@ip}:#{@port}"
      $connections.delete [@ip,@port]
    end
  end
  alias rules lambda
end

