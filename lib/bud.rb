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
require 'bud/state'

class Bud
  attr_reader :strata, :budtime, :inbound
  attr_accessor :connections
  attr_reader :tables, :ip, :port # for  ging; remove me later
  
  include BudState
  include Anise
  annotator :declare

  def initialize(ip, port, options = nil)
    @tables = {}
    @table_meta = []
    @strata = []
    @rewritten_strata = []
    @channels = {}
    @budtime = 0
    @ip = ip
    @port = port.to_i
    @ip_port = "#{@ip}:#{@port}"
    @connections = {}
    @inbound = []
    @declarations = []
    @options = options.nil? ? {} : options
    self.class.ancestors.each do |anc|
      @declarations += anc.annotation.map{|a| a[0] if a[1].keys.include? :declare}.compact if anc.methods.include? 'annotation'
    end
    #self.class.annotation.map {|a| print "another annotation: #{a.inspect}\n" } 
    @declarations.uniq!

    @periodics = table :periodics_tbl, ['name'], ['ident', 'duration']
    @vars = table :vars_tbl, ['name'], ['value']
    @tmpvars = scratch :tmpvars_tbl, ['name'], ['value']

    state

    bootstrap
    
    # meta stuff.  parse the AST of the current (sub)class,
    # get dependency info, and determine stratification order.
    unless self.class <= Stratification
      safe_rewrite
      provenance_extend if @options['provenance']
    end
   
  end
  
  ########### give empty defaults for these
  def state
    #channel :tickler, 0, ['server']
  end  
  def declaration
  end
  def bootstrap
  end

  ########### metaprogramming support for ruby and for rule rewriting
  # helper to define instance methods
  def singleton_class
    class << self; self; end
  end

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
    # for now
    @t.abort_on_exception = true
    # not clean
    sleep 0.1
  end

  def stop_bg
    @t.stop
  end

  def run
    begin 
      EventMachine::run {
        EventMachine::start_server(@ip, @port, Server, self) { |con|
          #          con.bud = self # pass this Bud object into the connection
        }
        # initialize periodics
        @periodics.each do |p|
          set_timer(p.name, p.ident, p.duration)
        end
        builtin_state
        tick
      }
    end
  end

  def builtin_state
    channel  :localtick, ['col1']
    terminal :stdio
  end

  def tick
    # reset any schema stuff that isn't already there
    # state to be defined by the user program
    # rethink this.
    unless @options['provenance']
      state
      builtin_state
    end
    
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
    @budtime += 1
    return @budtime
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
        @tables[p.name] = scratch p.name, @tables[p.name].keys, @tables[p.name].cols
      end
    end
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

