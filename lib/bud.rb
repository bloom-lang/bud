require 'rubygems'
require 'anise'
require 'eventmachine'
require 'msgpack'
require 'socket'
require 'superators'
require 'thread'

require 'bud/aggs'
require 'bud/bud_meta'
require 'bud/collections'
require 'bud/errors'
require 'bud/joins'
require 'bud/rtrace'
require 'bud/server'
require 'bud/state'
require 'bud/storage/tokyocabinet'
require 'bud/storage/zookeeper'
require 'bud/viz'

module BudModule
  def self.included(o)
    # Add support for the "declare" annotator to the specified module
    o.send(:include, Anise)
    o.send(:annotator, :declare)

    # Transform "state" and "bootstrap" blocks (calls to a module methods with
    # that name) into instance methods with a special name.
    def o.state(&block)
      meth_name = "__#{self}__state".to_sym
      define_method(meth_name, &block)
    end
    def o.bootstrap(&block)
      meth_name = "__#{self}__bootstrap".to_sym
      define_method(meth_name, &block)
    end

    # NB: it would be easy to workaround this by creating an alias for the
    # user's included method and then calling the alias from our replacement
    # "included" method.
    if o.singleton_methods.include? "included"
      # XXX: If o is a subclass of Bud, it already has a definition of the
      # included method, so avoid complaining or defining a duplicate.
      # return if o < Bud
      # raise "#{o} already defines 'included' singleton method!"
      return
    end

    # If Module X includes BudModule and Y includes X, we want BudModule's
    # "included" method to be invoked for both X and Y.
    def o.included(other)
      BudModule.included(other)
    end
  end
end

# The root Bud module. To run a Bud instance, there are three main options:
#
# 1. Synchronously. To do this, instanciate your program and then call tick()
#    one or more times; each call evaluates a single Bud timestep. Note that in
#    this mode, network communication (channels) and timers cannot be used. This
#    is mostly intended for "one-shot" programs that compute a single result and
#    then terminate.
# 2. In a separate thread in the foreground. To do this, instanciate your
#    program and then call run(). The Bud interpreter will then run, handling
#    network events and evaluating new timesteps as appropriate. The run()
#    method will not return unless an error occurs.
# 3. In a separate thread in the background. To do this, instanciate your
#    program and then call run_bg(). The Bud interpreter will run
#    asynchronously. To interact with Bud (e.g., insert additional data or
#    inspect the state of a Bud collection), use the sync_do and async_do
#    methods. To shutdown the Bud interpreter, use stop_bg().
#
# Most programs should use method #3.
#
# :main: Bud
module Bud
  attr_reader :strata, :budtime, :inbound, :options, :meta_parser, :viz, :rtracer
  attr_reader :dsock
  attr_reader :tables, :ip, :port
  attr_reader :stratum_first_iter

  include BudModule
  include BudState

  def initialize(options={})
    @tables = {}
    @table_meta = []
    @rewritten_strata = []
    @channels = {}
    @tc_tables = {}
    @zk_tables = {}
    @timers = []
    @budtime = 0
    @inbound = []
    @declarations = []
    @done_bootstrap = false

    # Setup options (named arguments), along with default values
    @options = options
    @options[:ip] ||= "localhost"
    @ip = @options[:ip]
    @options[:port] ||= 0
    @options[:port] = @options[:port].to_i
    # NB: If using an ephemeral port (specified by port = 0), the actual port
    # number won't be known until we start EM

    self.class.ancestors.each do |anc|
      if anc.methods.include? 'annotation'
        @declarations += anc.annotation.map{|a| a[0] if a[1].keys.include? :declare}.compact
      end
    end
    @declarations.uniq!

    @state_methods = lookup_state_methods

    init_state

    # NB: Somewhat hacky. Dependency analysis and stratification are implemented
    # by Bud programs, so in order for those programs to parse, we need the
    # "Bud" class to have been defined first.
    require 'bud/depanalysis'
    require 'bud/stratify'

    @viz = VizOnline.new(self) if @options[:trace]
    @rtracer = RTrace.new(self) if @options[:rtrace]

    # meta stuff.  parse the AST of the current (sub)class,
    # get dependency info, and determine stratification order.
    unless self.class <= Stratification or self.class <= DepAnalysis
      do_rewrite
    end

    # Load the rules as a closure.
    @strata = []
    declaration
    @rewritten_strata.each_with_index do |rs, i|
      block = eval "lambda { #{rs} }"
      @strata << block
    end
  end

  private

  def lookup_state_methods
    rv = []

    # Traverse the ancestor hierarchy from root => leaf. This helps to support a
    # common idiom: the schema of a table in a child module/class might
    # reference the schema of an included module.
    self.class.ancestors.reverse.each do |anc|
      meth_name = anc.instance_methods.find {|m| m == "__#{anc}__state"}
      if meth_name
        rv << self.method(meth_name.to_sym)
      end
    end
    rv
  end

  # Evaluate all bootstrap blocks
  def do_bootstrap
    self.class.ancestors.reverse.each do |anc|
      meth_name = anc.instance_methods.find {|m| m == "__#{anc}__bootstrap"}
      if meth_name
        self.method(meth_name.to_sym).call
      end
    end
    bootstrap

    @done_bootstrap = true
  end

  def do_rewrite
    @meta_parser = BudMeta.new(self, @declarations)
    @rewritten_strata = @meta_parser.meta_rewrite
  end

  public

  ########### give empty defaults for these
  def declaration
  end
  def bootstrap
  end

  ########### metaprogramming support for ruby and for rule rewriting
  # helper to define instance methods
  def singleton_class
    class << self; self; end
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

  # Run Bud in the "foreground" -- the caller's thread will be used to run the
  # Bud interpreter. This means this method won't return unless an error
  # occurs. It is often more useful to run Bud asynchronously -- see run_bg.
  #
  # Note that run cannot be invoked if run_bg has already been called in the
  # same Ruby process.
  #
  # Execution proceeds in time ticks, a la Dedalus.
  # * Within each tick there may be multiple strata.
  # * Within each stratum we do multiple semi-naive iterations.
  def run
    raise BudError if EventMachine::reactor_running?

    EventMachine::run {
      start_bud
    }
  end

  # Shutdown a Bud instance running asynchronously. This method blocks until Bud
  # has been shutdown. If _stop_em_ is true, the EventMachine event loop is also
  # shutdown; this will interfere with the execution of any other Bud instances
  # in the same process (as well as anything else that happens to use EM).
  def stop_bg(stop_em=false)
    schedule_and_wait do
      do_shutdown(stop_em)
    end
  end

  # Given a block, evaluate that block inside the background Ruby thread at some
  # point in the future. Because the callback is invoked inside the background
  # Ruby thread, Bud state can be safely examined inside the block. Naturally,
  # this method can only be used when Bud is running in the background. Note
  # that calling sync_do blocks the caller's thread until the block has been
  # evaluated by the Bud thread; for a non-blocking version, see async_do.
  #
  # Note that the callback is invoked after one Bud timestep has ended but
  # before the next timestep begins. Hence, synchronous accumulation (<=) into a
  # Bud scratch collection in a callback is typically not useful: when the next
  # tick begins, the content of any scratch collections will be empted, which
  # includes anything inserted by a sync_do block using <=. To avoid this
  # behavior, insert into scratches using <+.
  def sync_do
    schedule_and_wait do
      yield if block_given?
      # Do another tick, in case the user-supplied block inserted any data
      tick
    end
  end

  # Like sync_do, but does not block the caller's thread: the given callback
  # will be invoked at some future time. Note that calls to async_do respect
  # FIFO order.
  def async_do
    EventMachine::schedule do
      yield if block_given?
      # Do another tick, in case the user-supplied block inserted any data
      tick
    end
  end

  # Shutdown any persistent tables used by the current Bud instance. If you are
  # running Bud via tick() and using `tctable` collections, you should call this
  # after you're finished using Bud. Programs that use Bud via run() or run_bg()
  # don't need to call this manually.
  def close_tables
    @tables.each_value do |t|
      t.close
    end
  end

  private

  def start_reactor
    return if EventMachine::reactor_running?

    EventMachine::error_handler do |e|
      puts "Unexpected Bud error: #{e.inspect}"
      puts e.backtrace.join("\n")
      raise e
    end

    # Block until EM has successfully started up.
    q = Queue.new
    Thread.new do
      EventMachine.run do
        q << true
      end
    end
    q.pop
  end

  # Schedule a block to be evaluated by EventMachine in the future, and
  # block until this has happened.
  def schedule_and_wait
    # Try to defend against error situations in which EM has stopped, but we've
    # been called nonetheless. This is racy, but better than nothing.
    raise BudError, "EM not running" unless EventMachine::reactor_running?

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

  def do_shutdown(stop_em=false)
    @timers.each do |t|
      t.cancel
    end
    close_tables
    @dsock.close_connection
    # Note that this affects anyone else in the same process who happens to be
    # using EventMachine!
    EventMachine::stop_event_loop if stop_em
  end

  # Schedule a "graceful" shutdown for a future EM tick. If EM is not currently
  # running, shutdown immediately.
  def schedule_shutdown(stop_em=false)
    if EventMachine::reactor_running?
      EventMachine::schedule do
        do_shutdown(stop_em)
      end
    else
      do_shutdown(stop_em)
    end
  end

  def start_bud
    raise BudError unless EventMachine::reactor_thread?

    # If we get SIGINT or SIGTERM, shutdown gracefully
    Signal.trap("INT") do
      schedule_shutdown(true)
    end
    Signal.trap("TRAP") do
      schedule_shutdown(true)
    end

    do_start_server

    # Initialize periodics
    @periodics.each do |p|
      @timers << set_periodic_timer(p.pername, p.ident, p.period)
    end

    # Compute a fixpoint; this will also invoke any bootstrap blocks.
    tick

    @rtracer.sleep if options[:rtrace]
  end

  def do_start_server
    if @options[:port] == 0
      @dsock = EventMachine::open_datagram_socket(@ip, 0, BudServer, self)
      @port = Socket.unpack_sockaddr_in(@dsock.get_sockname)[0]
    else
      @port = @options[:port]
      @dsock = EventMachine::open_datagram_socket(@ip, @port, BudServer, self)
    end
  end

  public

  def ip_port
    raise BudError, "ip_port called before port defined" if @port.nil? and @options[:port] == 0
    @port.nil? ? "#{@ip}:#{@options[:port]}" : "#{@ip}:#{@port}"
  end

  def tick
    @tables.each_value do |t|
      t.tick
    end

    do_bootstrap unless @done_bootstrap
    receive_inbound

    @strata.each { |strat| stratum_fixpoint(strat) }
    @viz.do_cards if @options[:trace]
    do_flush
    @budtime += 1
  end

  private

  # Builtin BUD state (predefined collections). We could define this using the
  # standard "state" syntax, but we want to ensure that builtin state is
  # initialized before user-defined state.
  def builtin_state
    channel  :localtick, [:col1]
    terminal :stdio
    @periodics = table :periodics_tbl, [:pername] => [:ident, :period]

    # for BUD reflection
    table :t_rules, [:rule_id] => [:lhs, :op, :src]
    table :t_depends, [:rule_id, :lhs, :op, :body] => [:nm]
    table :t_depends_tc, [:head, :body, :via, :neg, :temporal]
    table :t_provides, [:interface] => [:input]
    table :t_stratum, [:predicate] => [:stratum]
    table :t_cycle, [:predicate, :via, :neg, :temporal]
  end

  # Invoke all the user-defined state blocks and initialize builtin state.
  def init_state
    builtin_state
    @state_methods.each do |s|
      s.call
    end
  end

  # Handle any inbound tuples off the wire and then clear. Received messages are
  # placed directly into the storage of the appropriate local channel.
  def receive_inbound
    @inbound.each do |msg|
      #      puts "dequeueing tuple #{msg[1].inspect} into #{msg[0]} @ #{ip_port}"
      tables[msg[0].to_sym] << msg[1]
    end
    @inbound = []
  end

  # "Flush" any tuples that need to be flushed. This does two things:
  # 1. Emit outgoing tuples in channels and ZK tables.
  # 2. Commit to disk any changes made to on-disk tables.
  def do_flush
    @channels.each { |c| @tables[c[0]].flush }
    @zk_tables.each_value { |t| t.flush }
    @tc_tables.each_value { |t| t.flush }
  end

  def stratum_fixpoint(strat)
    # This routine uses semi-naive evaluation to compute
    # a fixpoint of the rules in strat.
    #
    # As described in lib/collections.rb, each collection has three
    # sub-collections of note here:
    #   @storage: the "main" storage of tuples
    #   @delta: tuples that should be used to drive derivation of new facts
    #   @new_delta: a place to store newly-derived facts
    #
    # The first time through this loop we mark @stratum_first_iter=true,
    # while tells the Join::each code to join up all its @storage subcollections
    # to start. In subsequent iterations the join code uses some table's @delta
    # to ensure that only new tuples are derived.
    #
    # Note that calling "each" on a non-Join collection will iterate through both
    # storage and delta.
    #
    # At the end of each iteration of this loop we transition:
    # - @delta tuples are merged into @storage
    # - @new_delta tuples are moved into @delta
    # - @new_delta is set to empty
    #
    # XXX as a performance optimization, it would be nice to bypass the delta
    # tables for any preds that don't participate in a rhs Join -- in that
    # case there's pointless extra tuple movement letting tuples "graduate"
    # through @new_delta and @delta.

    # In semi-naive, the first iteration should join up tables
    # on their storage fields; subsequent iterations do the
    # delta-joins only.  The stratum_first_iter field here distinguishes
    # these cases.
    @stratum_first_iter = true
    begin
      strat.call
      @stratum_first_iter = false
      # XXX this next line is inefficient.
      # we could call tick_deltas only on predicates in this stratum.
      # but it's not easy right now (??) to pull out tables in a given stratum
      @tables.each{|name,coll| coll.tick_deltas}
    end while not @tables.all?{|name,coll| coll.new_delta.empty? and coll.delta.empty?}
  end

  ####### Joins
  private
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

  def wrap_map(j, &blk)
    if blk.nil?
      return j
    else
      return j.map(&blk)
    end
  end

  public
  def join(rels, *preds, &blk)
    j = BudJoin.new(rels, self, decomp_preds(*preds))
    wrap_map(j, &blk)
  end

  alias coincide join

  def natjoin(rels, &blk)
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
    j = join(rels, *preds)
    wrap_map(j, &blk)
  end

  # ugly, but why not
  alias natcoincide natjoin

  def leftjoin(rels, *preds, &blk)
    j = BudLeftJoin.new(rels, self, decomp_preds(*preds))
    wrap_map(j, &blk)
  end

  # ugly, but why not
  alias leftcoincide leftjoin

  private

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
end
