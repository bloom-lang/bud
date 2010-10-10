require 'enumerator'
require 'msgpack'
require 'eventmachine'
require 'socket'
require 'superators'
require 'parse_tree'
require 'parse_tree_extensions'
require 'anise'
require 'bud/aggs'
require 'bud/collections'
require 'bud/errors'
require 'bud/events'
require 'bud/strat'
require 'bud/static_analysis'
require 'bud/bud_meta'
require 'bud/viz'

class Bud
  attr_reader :strata, :budtime, :inbound
  attr_accessor :connections
  attr_reader :tables, :ip, :port # for  ging; remove me later
  
  include Anise
  annotator :declare

  def initialize(ip, port)
    @tables = {}
    @table_meta = []
    @strata = []
    @rewritten_strata = []
    @channels = {}
    @budtime = 0
    @ip = ip
    @port = port.to_i
    @connections = {}
    @inbound = []
    @declarations = []
    self.class.ancestors.each do |anc|
      @declarations += anc.annotation.map{|a| a[0] if a[1].keys.include? :declare}.compact if anc.methods.include? 'annotation'
    end
    @declarations.uniq!

    @periodics = table :periodics_tbl, ['name'], ['ident', 'duration']
    @vars = table :vars_tbl, ['name'], ['value']
    @tmpvars = scratch :tmpvars_tbl, ['name'], ['value']

    # meta stuff.  parse the AST of the current (sub)class,
    # get dependency info, and determine stratification order.
    unless self.class <= Stratification
      safe_rewrite
    end
  end
  
  ########### give empty defaults for these
  def state
    #channel :tickler, 0, ['server']
  end  
  def declaration
  end

  def singleton_class
    class << self; self; end
  end

  ########### metaprogramming support for ruby and for rule rewriting
  # helper to define instance methods

    

  def safe_rewrite
    begin
      @rewritten_strata = meta_rewrite
    rescue 
      print "Running original(#{self.class.to_s}) code: couldn't rewrite stratified ruby (#{$!})\n"
    end 
  end

  ######## methods for controlling execution
  def run_bg
    @t = Thread.new() do ||
      # PAA, towards better error messages
      begin
        run
      rescue
        print "background thread failed with #{$!}\n"
        exit
      end
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

    #print "#{self.class}.tick: budtime #{@budtime}\n"
    state

    receive_inbound

    # load the rules as a closure (will contain persistent tuples and new inbounds)
    # declaration to be provided by user program
    @strata = []
    declaration
    if @rewritten_strata.length > 0 
      @rewritten_strata.each_with_index do |rs, i|
        # FIX: move to compilation
        str = rs.nil? ? "" : rs
        block = lambda { eval(str) } 
        @strata << block 
      end
    elsif @declarations.length > 0
      # the old way...
      @declarations.each do |d|
        @strata << self.method(d).to_proc
      end
    end 
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
      self.singleton_class.send(:define_method, name) do 
        @tables[name]
      end 
      return nil
    end
  end

  def table(name, keys=[], cols=[], conf=nil)
    check_table(name, keys, cols)
    @tables[name] ||= BudTable.new(name, keys, cols, self, conf)
  end

  def blackboard(name, keys=[], cols=[])
    table(name, keys, cols, "last")
  end

  def permanent(name, keys=[], cols=[])
    table(name, keys, cols, "first")
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

  # methods to define vars and tmpvars.  This code still quite tentative
  def regvar(name, collection)
    # rule out varnames that used reserved words
    reserved = defined?(name)
    if reserved == "method" and not collection[name] then
      # first time registering var, check for method name reserved
      raise BudError, "symbol :#{name} reserved, cannot be used as variable name"
    end
    self.singleton_class.send :define_method, name do 
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
  def decomp_preds(*preds)
    # decompose each pred into a binary pred
    newpreds = []
    preds.each do |p|
      p.each_with_index do |c, i|
        newpreds << [p[i], p[i+1]] unless p[i+1].nil?
      end
    end
    newpreds
  end
  
  def join(rels, *preds)
    BudJoin.new(rels, decomp_preds(*preds))
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
  
  def leftjoin(rels, *preds)
    BudLeftJoin.new(rels, decomp_preds(*preds))
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

