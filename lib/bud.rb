require 'rubygems'
require 'anise'
require 'eventmachine'
require 'msgpack'
require 'superators'
require 'bud/aggs'
require 'bud/bud_meta'
require 'bud/collections'
require 'bud/errors'
require 'bud/server'
require 'bud/state'
require 'bud/strat'
require 'bud/viz'

class Bud
  attr_reader :strata, :budtime, :inbound, :options, :time_pics_dir, :provides, :meta_parser, :viz, :server
  attr_accessor :connections
  attr_reader :tables, :ip, :port
  attr_reader :stratum_first_iter

  include BudState
  include Anise
  annotator :declare

  def initialize(options={})
    @tables = {}
    @table_meta = []
    @strata = []
    @rewritten_strata = []
    @provides = {}
    @channels = {}
    @tc_tables = {}
    @zk_tables = {}
    @timers = []
    @budtime = 0
    @connections = {}
    @inbound = []
    @declarations = []
    @server = nil

    # Setup options (named arguments), along with default values
    @options = options
    @options[:ip] ||= "localhost"
    @ip = @options[:ip]
    @options[:port] ||= 0
    @options[:port] = @options[:port].to_i

    # NB: If using an ephemeral port (specified by port = 0), the actual port
    # number may not be known until we start EM

    self.class.ancestors.each do |anc|
      @declarations += anc.annotation.map{|a| a[0] if a[1].keys.include? :declare}.compact if anc.methods.include? 'annotation'
    end
    @declarations.uniq!

    init_state
    bootstrap
    @viz = Viz.new(self)
    @viz.prepare_viz

    # make sure that new_delta tuples from bootstrap rules are transitioned into
    # storage before first tick.
    tables.each{|name,coll| coll.install_deltas}
    # note that any tuples installed into a channel won't immediately be
    # flushed; we need to wait for EM startup to do that

    # meta stuff.  parse the AST of the current (sub)class,
    # get dependency info, and determine stratification order.
    unless self.class <= Stratification or self.class <= DepAnalysis
      safe_rewrite
    end
  end

  ########### give empty defaults for these
  def state
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
    if @options[:disable_rewrite]
      puts "No rewriting performed"
      return
    end

    @meta_parser = BudMeta.new(self, @declarations)
    begin
      @rewritten_strata = @meta_parser.meta_rewrite
    rescue Exception
      raise if @options[:enforce_rewrite]
      puts "Running original (#{self.class}) code: couldn't rewrite stratified ruby (#{$!})"
    end
    @viz.visualize(@meta_parser.strat_state, "#{self.class}_gvoutput", @meta_parser.rules, @meta_parser.depanalysis) if @options[:visualize]
  end

  ######## methods for controlling execution

  # Run Bud in the background (in a different thread). This means that the Bud
  # interpreter will run asynchronously from the caller, so care must be used
  # when interacting with it. For example, it is not safe to directly examine
  # Bud collections from the caller's thread (see async_do and sync_do).
  #
  # This instance of Bud will continue to run until stop_bg is called.
  def run_bg
    start_reactor

    # Wait for Bud to start up before returning
    schedule_and_wait do
      start_bud
    end
  end

  def start_reactor
    return if EventMachine::reactor_running?

    Thread.new do
      EventMachine.run
    end
  end

  # Shutdown a Bud instance running asynchronously. This method blocks until Bud
  # has been shutdown.
  def stop_bg
    schedule_and_wait do
      do_shutdown
    end
  end

  # Given a block, evaluate that block inside the background Ruby thread at some
  # point in the future. Because the background Ruby thread is blocked, Bud
  # state can be safely examined inside the block. Naturally, this method can
  # only be used when Bud is running in the background. Note that calling
  # async_do returns immediately; the callback is invoked at some future time.
  def async_do
    EventMachine::schedule do
      yield
      # Do another tick, in case the user-supplied block inserted any data
      tick
    end
  end

  # Like async_do, but provides syntax sugar for a common case: the calling
  # thread is blocked until the supplied block has been evaluated by the
  # Bud thread. Note that calls to sync_do and async_do respect FIFO order.
  def sync_do
    schedule_and_wait do
      yield
      # Do another tick, in case the user-supplied block inserted any data
      tick
    end
  end

  # Schedule a block to be evaluated by EventMachine in the future, and
  # block until this has happened.
  def schedule_and_wait
    # Ruby doesn't provide semaphores (!), so we use thread-safe queues.
    q = Queue.new
    EventMachine::schedule do
      ret = false
      begin
        yield
      rescue Exception
        ret = $!
      end
      q.push(ret)
    end

    resp = q.pop
    raise resp if resp
  end

  def close_tables
    @tables.each_value do |t|
      t.close
    end
  end

  def do_shutdown(stop_em = false)
    @timers.each do |t|
      t.cancel
    end
    @connections.each_value do |c|
      c.close_connection
    end
    close_tables
    EventMachine::stop_server @server
    EventMachine::stop_event_loop if stop_em
  end

  # Schedule a "graceful" shutdown for a future EM tick.
  def schedule_shutdown(stop_em = false)
    EventMachine::schedule do
      do_shutdown(stop_em)
    end
  end

  def start_bud
    raise unless EventMachine::reactor_thread?

    # If we get SIGINT or SIGTERM, shutdown gracefully
    Signal.trap("INT") do
      schedule_shutdown(true)
    end
    Signal.trap("TRAP") do
      schedule_shutdown(true)
    end

    do_start_server

    # flush any tuples installed into channels during bootstrap block
    # XXX: doing this here is a kludge; we should do all of bootstrap
    # in one place
    do_flush

    # initialize periodics
    @periodics.each do |p|
      @timers << set_periodic_timer(p.pername, p.ident, p.period)
    end
    tick
  end

  # Run Bud in the "foreground" -- this method typically doesn't return unless
  # an error occurs.
  #
  # We proceed in time ticks, a la Dedalus.
  # * Within each tick there may be multiple strata.
  # * Within each stratum we do multiple semi-naive iterations.
  def run
    raise if EventMachine::reactor_running?

    EventMachine::run {
      start_bud
    }
  end

  def do_start_server
    # XXX: EM doesn't really support binding to an ephemeral port at the moment
    # (it provides no way to determine which port number was chosen), so for now
    # we emulate this by attempting to bind to randomly-chosen ports until we
    # find a free one.
    if @options[:port] == 0
      success = false
      15.times do
        @port = 5000 + rand(20000)
        begin
          @server = EventMachine::start_server(@ip, @port, BudServer, self)
          success = true
          break
        rescue Exception
          next
        end
      end
      raise "Failed to bind to local TCP port" unless success
    else
      @port = @options[:port]
      @server = EventMachine::start_server(@ip, @port, BudServer, self)
    end
  end

  def ip_port
    "#{@ip}:#{@port}"
  end

  # "Flush" any tuples that need to be flushed. This does two things:
  # 1. Emit outgoing tuples in channels and ZK tables.
  # 2. Commit to disk any changes made to on-disk tables.
  def do_flush
    @channels.each { |c| @tables[c[0]].flush }
    @zk_tables.each_value { |t| t.flush }
    @tc_tables.each_value { |t| t.flush }
  end

  def builtin_state
    channel  :localtick, ['col1']
    terminal :stdio
    @periodics = table :periodics_tbl, ['pername'], ['ident', 'period']
    @vars = table :vars_tbl, ['varname'], ['value']
    @tmpvars = scratch :tmpvars_tbl, ['tmpvarname'], ['value']
    
  end

  def init_state
    # For every state declaration, either define a new collection instance
    # (first time seen) or tick the collection to advance to the next time step.
    builtin_state
    state
  end

  def tick
    init_state

    receive_inbound
    @tables.each do |name,coll|
      coll.tick_deltas
    end

    # Load the rules as a closure (will contain persistent tuples and new inbounds)
    # declaration is gathered from "declare def" blocks
    # XXX: can we kill the old "declaration" code?
    @strata = []
    declaration
    if @rewritten_strata.length > 0
      @rewritten_strata.each_with_index do |rs, i|
        # FIX: move to compilation
        str = rs.nil? ? "" : rs
        # eval once and put into block
        block = eval "lambda { #{str} }"
        @strata << block
      end
    elsif @declarations.length > 0
      # the old way...
      @declarations.each do |d|
        @strata << self.method(d).to_proc
      end
    end
    @strata.each { |strat| stratum_fixpoint(strat) }
    @viz.do_cards
    do_flush
    @budtime += 1
  end

  # handle any inbound tuples off the wire and then clear
  def receive_inbound
    @inbound.each do |msg|
#      puts "dequeueing tuple #{msg[1].inspect} into #{msg[0]} @ #{ip_port}"
      tables[msg[0].to_sym] << msg[1]
    end
    @inbound = []
  end

  def stratum_fixpoint(strat)
    # This routine uses semi-naive evaluation to compute
    # a fixpoint of the rules in strat.
    # We *almost* have semi-naive evaluation working.
    # at end of each iteration of this loop we transition:
    # - delta tuples move into storage
    # - new_delta moves to delta
    # - new_delta is set to empty
    # (see BudCollection for a description of the 4 partitions
    #  of tuples within a collection.)
    # This scheme does semi-naive eval for Join.map
    # because the join.each code understands
    # the diff between storage and delta.
    # But calling map on a non-join collection goes through both
    # storage and delta.

    # XXX
    # To use deltas for all Collections (not just Join), we would
    # need Collection.each to understand that on iteration 1 of a
    # fixpoint, it should use storage for all predicates, but
    # on iterations 2..n of a fixpoint, it should use
    # deltas for predicates that appear in lhs in this stratum,
    # and use storage for predicates that appear in lower strata.
    # XXX
    # another performance optimization would be to bypass the delta
    # tables for any preds that don't participate in an rhs -- in that
    # case the deltas just end up requiring pointless extra tuple movement


    # In semi-naive, the first iteration should join up tables
    # on their storage fields; subsequent iterations do the
    # delta-joins only.  The stratum_first_iter field here distinguishes
    # these cases.
    @stratum_first_iter = true
    begin
      strat.call
      @stratum_first_iter = false
      # this is overkill.
      # should call tick_deltas only on predicates in this stratum
      # and then should appropriately deal with deltas in subsequent strata.
      @tables.each{|name,coll| coll.tick_deltas}
    end while not @tables.all?{|name,coll| coll.new_delta.empty? and coll.delta.empty?}
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
    BudJoin.new(rels, self, decomp_preds(*preds))
  end

  def natjoin(rels)
    # for all pairs of relations, add predicates on matching column names
    preds = []
    rels.each do |r|
      rels.each do |s|
        matches = r.schema & s.schema
        matches.each do |c|
          preds << [self.send(r.tabname).send(c), self.send(s.tabname).send(c)] unless r.tabname.to_s >= s.tabname.to_s
        end
      end
    end
    preds.uniq!
    join(rels, *preds)
  end

  def leftjoin(rels, *preds)
    BudLeftJoin.new(rels, self, decomp_preds(*preds))
  end

  ######## ids and timers
  def gen_id
    Time.new.to_i.to_s << rand.to_s
  end

  def set_periodic_timer(name, id, period)
    EventMachine::PeriodicTimer.new(period) do
      @tables[name] <+ [[id, Time.new.to_s]]
      tick
    end
  end

  alias rules lambda
end
