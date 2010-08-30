require 'enumerator'
require 'msgpack'
require 'eventmachine'
require 'socket'
require 'superators'
require 'parse_tree'
require 'parse_tree_extensions'
require 'bud/aggs'
require 'bud/collections'
require 'bud/errors'
require 'bud/events'
require 'bud/parse_bud'
require 'bud/strat'
require 'bud/forward_parse'

class Bud
  attr_reader :strata, :budtime, :inbound
  attr_accessor :connections
  attr_reader :tables # for  ging; remove me later

  def initialize(ip, port)
    @tables = {}
    @strata = []
    @channels = {}
    @budtime = 0
    @ip = ip
    @port = port.to_i
    @connections = {}
    @inbound = []

    @periodics = table :periodics_tbl, ['name'], ['ident', 'duration']
    @vars = table :vars_tbl, ['name'], ['value']
    @tmpvars = scratch :tmpvars_tbl, ['name'], ['value']
    @depends = table :depends, ['head', 'op', 'body', 'neg']

    # meta stuff.  parse the AST of the current (sub)class,
    # get dependency info, and determine stratification order.
    if self.class != Stratification
      # N.B. -- parse_tree will not be supported in ruby 1.9.
      # however, we can still pass the "string" code of bud modules
      # to ruby_parse (but not the "live" class)
      # uncomment for stratification rewrites
      ##safe_rewrite
    end
  end

  def safe_rewrite
    begin
      defn = meta_rewrite 
      # uncomment to see the rewrite -- it has already been installed if it succeeded.
      # puts defn
    rescue 
      print "Running original code: couldn't rewrite stratified ruby (#{$!})\n"
    end 
  end

  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)

    copy1 = ParseTree.translate(self.class);
    r2r = MyR2R.new
    ruby = r2r.process(copy1)
 
    # because r2r appears to stomp on its input
    copy2 = ParseTree.translate(self.class)
    bm = BudMirror.new
    result = bm.process(copy2)

    strat = Stratification.new("localhost", 12345)
    bm.each_depends { |d| strat.depends << d } 
    strat.tick
    strat.stratum.each do |d|
      r2r.tabstuff.add_strat(d[0], d[1])
    end

    defn = ''      
    (0..r2r.tabstuff.strata.length-1).each do |s|
      defn = defn + ('strata[' + s.to_s + '] = rules {')
      r2r.tabstuff.strata_each(s) do |t|
        defn = defn + t + "\n"
      end
      defn = defn + "}\n"
    end
    self.class.send(:remove_method, "declaration")
    l = lambda { eval(defn) }
    self.class.send(:define_method, "declaration",  l)
    return defn
  end  

  ######## methods for controlling execution
  def run_bg
    @t = Thread.new() do ||
      run
    end
    # not clean
    sleep 1
  end
  def run
    begin 
      EventMachine::run {
        EventMachine::start_server(@ip, @port, Server, self) { |con|
#          con.bud = self # pass this Bud object into the connection
        }
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
    @inbound.each do |msg|
      tables[msg[0].to_sym] << msg[1]
    end
    @inbound = []
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
  def check_table(name, keys=[], cols=[])
    # rule out tablenames that used reserved words
    reserved = eval "defined?(#{name})"
    unless (reserved.nil? or (reserved == "method" and @tables[name]))
      # first time registering table, check for method name reserved
      raise BudError, "symbol :#{name} reserved, cannot be used as table name"
    end

    # tick previously-defined tables and tick
    if @tables[name] then
      # check for consistent redefinition, and "tick" the table
      if @tables[name].keys != keys or @tables[name].cols != cols then
        raise BudError, "create :#{name}, keys = #{keys.inspect}, cols = #{cols.inspect} \n \
        table :#{name} already defined as #{@tables[name].keys.inspect} #{@tables[name].cols.inspect}"
      end
      @tables[name].tick
      return @tables[name]
    else 
      self.class.send :define_method, name do 
        @tables[name]
      end 
      return nil
    end
  end
  
  def table(name, keys=[], cols=[], conf=nil)
    check_table(name, keys, cols)
    @tables[name] ||= BudTable.new(name, keys, cols, self, conf)
  end

  def scratch(name, keys=[], cols=[])
    check_table(name, keys, cols)
    @tables[name] ||= BudScratch.new(name, keys, cols, self)
  end

  def serializer(name, keys=[], cols=[])
    check_table(name, keys, cols)
    @tables[name] ||= BudSerializer.new(name, keys, cols, self)
  end

  def channel(name, locspec, keys=[], cols=[])
    check_table(name, keys, cols)
    @channels[name] = locspec
    @tables[name] ||= BudChannel.new(name, keys, cols, locspec, self)
  end
  
  def file_reader(name, filename, delimiter='\n')
    check_table(name, ['lineno'], ['text'])
    @tables[name] ||= BudFileReader.new(name, filename, delimiter, self)
  end

  def periodic(name, duration)
    t = check_table(name, ['ident'], ['time'])
    @tables[name] ||= BudPeriodic.new(name, ['ident'], ['time'], self)
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
          preds << [self.send(r.name).send(c), self.send(s.name).send(c)] unless r.name.to_s >= s.name.to_s
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

  def tickle
    EventMachine::connect(@ip, @port) do |c|
      c.send_data(" ")
    end
  end

  alias rules lambda
end

