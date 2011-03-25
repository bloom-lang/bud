require 'rubygems'
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

# We monkeypatch Module to add support for four new module methods: import,
# state, bootstrap, and bloom.
class Module
  def import(spec)
    raise Bud::CompileError unless (spec.class <= Hash and spec.length == 1)
    mod, local_name = spec.first
    raise Bud::CompileError unless (mod.class <= Module and local_name.class <= Symbol)

    # To correctly expand qualified references to an imported module, we keep a
    # table with the local bind names of all the modules imported by this
    # module. To handle nested references (a.b.c.d etc.), the import table for
    # module X points to X's own nested import table.
    @bud_import_tbl ||= {}
    child_tbl = mod.bud_import_table
    raise Bud::CompileError if @bud_import_tbl.has_key? local_name
    @bud_import_tbl[local_name] = child_tbl.clone # XXX: clone needed?

    rewritten_mod_name = ModuleRewriter.do_import(self, mod, local_name)
    self.module_eval "include #{rewritten_mod_name}"
  end

  # Transform "state", "bootstrap" and "bloom" blocks (calls to module methods
  # with that name) into instance methods with a special name.
  def state(&block)
    meth_name = Module.make_state_meth_name(self)
    define_method(meth_name, &block)
  end

  def bootstrap(&block)
    meth_name = "__bootstrap__#{Module.get_class_name(self)}".to_sym
    define_method(meth_name, &block)
  end

  def bloom(block_name=nil, &block)
    # If no block name was specified, generate a unique name
    if block_name.nil?
      @block_id ||= 0
      block_name = "#{Module.get_class_name(self)}__#{@block_id.to_s}"
      @block_id += 1
    else
      unless block_name.class <= Symbol
        raise Bud::CompileError, "Bloom block names must be a symbol: #{block_name}"
      end
    end

    # Note that we don't encode the module name ("self") into the name of the
    # method. This allows named blocks to be overridden (via inheritance or
    # mixin) in the same way as normal Ruby methods.
    meth_name = "__bloom__#{block_name}"

    # Don't allow duplicate named bloom blocks to be defined within a single
    # module; this indicates a likely programmer error.
    if instance_methods(false).include? meth_name
      raise Bud::CompileError, "Duplicate named bloom block: '#{block_name}' in #{self}"
    end
    define_method(meth_name.to_sym, &block)
  end

  def bud_import_table
    @bud_import_tbl ||= {}
    @bud_import_tbl
  end

  private
  # Return a string with a version of the class name appropriate for embedding
  # into a method name. Annoyingly, if you define class X nested inside
  # class/module Y, X's class name is the string "Y::X". We don't want to define
  # method names with semicolons in them, so just return "X" instead.
  def self.get_class_name(klass)
    klass.name.split("::").last
  end

  # State method blocks are named using an auto-incrementing counter. This is to
  # ensure that we can rediscover the possible dependencies between these blocks
  # after module import (see Bud#call_state_methods).
  def self.make_state_meth_name(klass)
    @state_meth_id ||= 0
    r = "__state#{@state_meth_id}__#{Module.get_class_name(klass)}".to_sym
    @state_meth_id += 1
    return r
  end
end

# The root Bud module. To run a Bud instance, there are three main options:
#
# 1. Synchronously. To do this, instantiate your program and then call tick()
#    one or more times; each call evaluates a single Bud timestep. Note that in
#    this mode, network communication (channels) and timers cannot be used. This
#    is mostly intended for "one-shot" programs that compute a single result and
#    then terminate.
# 2. In a separate thread in the foreground. To do this, instantiate your
#    program and then call run(). The Bud interpreter will then run, handling
#    network events and evaluating new timesteps as appropriate. The run()
#    method will not return unless an error occurs.
# 3. In a separate thread in the background. To do this, instantiate your
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
  attr_accessor :lazy # This can be changed on-the-fly by REBL

  include BudState

  def initialize(options={})
    @tables = {}
    @table_meta = []
    @rewritten_strata = []
    @channels = {}
    @tc_tables = {}
    @zk_tables = {}
    @callbacks = {}
    @callback_id = 0
    @timers = []
    @budtime = 0
    @inbound = []
    @done_bootstrap = false
    @em_stopped = Queue.new

    # Setup options (named arguments), along with default values
    @options = options
    @lazy = @options[:lazy] ||= false
    @options[:ip] ||= "localhost"
    @ip = @options[:ip]
    @options[:port] ||= 0
    @options[:port] = @options[:port].to_i
    # NB: If using an ephemeral port (specified by port = 0), the actual port
    # number won't be known until we start EM

    rewrite_local_methods

    @declarations = ModuleRewriter.get_rule_defs(self.class)

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

  # Rewrite methods defined in the main Bud class to expand module
  # references. Imported modules are rewritten during the import process.
  def rewrite_local_methods
    self.class.instance_methods(false).each do |m|
      ast = ParseTree.translate(self.class, m)
      ast = Unifier.new.process(ast)

      expander = NestedRefRewriter.new(self.class.bud_import_table)
      ast = expander.process(ast)

      new_source = Ruby2Ruby.new.process(ast)
      self.class.module_eval new_source # Replace previous method def
    end
  end

  # Invoke all the user-defined state blocks and initialize builtin state.
  def init_state
    builtin_state
    call_state_methods
  end

  # If module Y is a parent module of X, X's state block might reference state
  # defined in Y. Hence, we want to invoke Y's state block first.  However, when
  # "import" and "include" are combined, we can't use the inheritance hierarchy
  # to do this. When a module Z is imported, the import process inlines all the
  # modules Z includes into a single module. Hence, we can no longer rely on the
  # inheritance hierarchy to respect dependencies between modules. To fix this,
  # we add an increasing ID to each state block's method name (assigned
  # according to the order in which the state blocks are defined); we then sort
  # by this order before invoking the state blocks.
  def call_state_methods
    meth_map = {} # map from ID => [Method]
    self.class.instance_methods.each do |m|
      next unless m =~ /^__state(\d+)__/
      id = Regexp.last_match.captures.first.to_i
      meth_map[id] ||= []
      meth_map[id] << self.method(m)
    end

    meth_map.keys.sort.each do |i|
      meth_map[i].each {|m| m.call}
    end
  end

  # Evaluate all bootstrap blocks
  def do_bootstrap
    self.class.ancestors.reverse.each do |anc|
      anc.instance_methods(false).each do |m|
        if /^__bootstrap__/.match m
          self.method(m.to_sym).call
        end
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

  # Shutdown a Bud instance that is running asynchronously. This method blocks
  # until Bud has been shutdown. If +stop_em+ is true, the EventMachine event
  # loop is also shutdown; this will interfere with the execution of any other
  # Bud instances in the same process (as well as anything else that happens to
  # use EventMachine).
  def stop_bg(stop_em=false)
    if stop_em
      schedule_shutdown(true)
      # Wait until EM has completely shutdown before we return.
      @em_stopped.pop
    else
      schedule_and_wait do
        do_shutdown(false)
      end
    end
  end

  # Given a block, evaluate that block inside the background Ruby thread at some
  # time in the future. Because the callback is invoked inside the background
  # Ruby thread, Bud state can be safely examined inside the block. Naturally,
  # this method can only be used when Bud is running in the background. Note
  # that calling sync_do blocks the caller's thread until the block has been
  # evaluated by the Bud thread; for a non-blocking version, see async_do.
  #
  # Note that the callback is invoked after one Bud timestep has ended but
  # before the next timestep begins. Hence, synchronous accumulation (<=) into a
  # Bud scratch collection in a callback is typically not useful: when the next
  # tick begins, the content of any scratch collections will be emptied, which
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

  # Register a new callback. Given the name of a Bud collection, this method
  # arranges for the given block to be invoked at the end of any tick in which
  # any tuples have been inserted into the specified collection. The code block
  # is passed the collection as an argument; this provides a convenient way to
  # examine the tuples inserted during that fixpoint. (Note that because the Bud
  # runtime is blocked while the callback is invoked, it can also examine any
  # other Bud state freely.)
  #
  # Note that registering callbacks on persistent collections (e.g., tables and
  # tctables) is probably not a wise thing to do: as long as any tuples are
  # stored in the collection, the callback will be invoked at the end of every
  # tick.
  def register_callback(tbl_name, &block)
    # We allow callbacks to be added before or after EM has been started. To
    # simplify matters, we start EM if it hasn't been started yet.
    start_reactor
    cb_id = nil
    schedule_and_wait do
      unless @tables.has_key? tbl_name
        raise Bud::BudError, "No such table: #{tbl_name}"
      end

      raise Bud::BudError if @callbacks.has_key? @callback_id
      @callbacks[@callback_id] = [tbl_name, block]
      cb_id = @callback_id
      @callback_id += 1
    end
    return cb_id
  end

  # Unregister the callback that has the given ID.
  def unregister_callback(id)
    schedule_and_wait do
      raise Bud::BudError unless @callbacks.has_key? id
      @callbacks.delete(id)
    end
  end

  # sync_callback supports synchronous interaction with bud modules.
  # The caller supplies the name of an input relation,
  # a set of tuples to insert, and an output relation on which  to 'listen.'
  # The call blocks until tuples are inserted into the output collection:
  # these are returned to the caller.
  def sync_callback(in_coll, tupleset, out_coll)
    q = Queue.new
    cb = register_callback(out_coll) do |c|
      q.push c.to_a
    end
    sync_do do 
      @tables[in_coll] <+ tupleset
    end
    result = q.pop
    unregister_callback(cb)
    yield result
  end

  private

  def invoke_callbacks
    @callbacks.each_value do |cb|
      tbl_name, block = cb
      tbl = @tables[tbl_name]
      unless tbl.empty?
        block.call(tbl)
      end
    end
  end

  def start_reactor
    return if EventMachine::reactor_running?

    EventMachine::error_handler do |e|
      puts "Unexpected Bud error: #{e.inspect}"
      puts e.backtrace.join("\n")
      raise e
    end

    # Block until EM has successfully started up.
    q = Queue.new
    # This thread helps us avoid race conditions on the start and stop of
    # EventMachine's event loop.
    Thread.new do
      EventMachine.run do
        q << true
      end
      # Executed only after EventMachine::stop_event_loop is done
      @em_stopped << true
    end
    # Block waiting for EM's event loop to start up.
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
    # using EventMachine! This is also a non-blocking call; to block until EM
    # has completely shutdown, we use the @em_stopped queue.
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
    unless @options[:no_signal_handlers]
      Signal.trap("INT") do
        schedule_shutdown(true)
      end
      Signal.trap("TRAP") do
        schedule_shutdown(true)
      end
    end

    do_start_server

    # Initialize periodics
    @periodics.each do |p|
      @timers << set_periodic_timer(p.pername, p.ident, p.period)
    end

    # Arrange for Bud to read from stdin if enabled. Note that we can't do this
    # earlier because we need to wait for EventMachine startup.
    @stdio.start_stdin_reader if @options[:read_stdin]

    # Compute a fixpoint; this will also invoke any bootstrap blocks.
    tick unless @lazy

    @rtracer.sleep if options[:rtrace]
  end

  def do_start_server
    @dsock = EventMachine::open_datagram_socket(@ip, @options[:port],
                                                BudServer, self)
    @port = Socket.unpack_sockaddr_in(@dsock.get_sockname)[0]
  end

  public

  # Returns the ip and port of the Bud instance.  In addition to the local IP
  # and port, the user may define an external IP and/or port. the external
  # version of each is returned if available.  If not, the local version is
  # returned.  There are use cases for mixing and matching local and external.
  # local_ip:external_port would be if you have local port forwarding, and
  # external_ip:local_port would be if you're in a DMZ, for example
  def ip_port
    raise BudError, "ip_port called before port defined" if @port.nil? and @options[:port] == 0 and not @options[:ext_port]

    ip = options[:ext_ip] ? "#{@options[:ext_ip]}" : "#{@ip}"
    port = options[:ext_port] ? "#{@options[:ext_port]}" :
      (@port.nil? ? "#{@options[:port]}" : "#{@port}")
    ip + ":" + port
  end

  # Returns the internal IP and port
  def int_ip_port
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
    invoke_callbacks
    @budtime += 1
  end

  private

  # Builtin BUD state (predefined collections). We could define this using the
  # standard "state" syntax, but we want to ensure that builtin state is
  # initialized before user-defined state.
  def builtin_state
    channel  :localtick, [:col1]
    @stdio = terminal :stdio
    @periodics = table :periodics_tbl, [:pername] => [:ident, :period]

    # for BUD reflection
    table :t_rules, [:rule_id] => [:lhs, :op, :src]
    table :t_depends, [:rule_id, :lhs, :op, :body] => [:nm]
    table :t_depends_tc, [:head, :body, :via, :neg, :temporal]
    table :t_provides, [:interface] => [:input]
    table :t_stratum, [:predicate] => [:stratum]
    table :t_cycle, [:predicate, :via, :neg, :temporal]
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
  def wrap_map(j, &blk)
    if blk.nil?
      return j
    else
      return j.map(&blk)
    end
  end

  public
  def join(rels, *preds, &blk)
    j = BudJoin.new(rels, self, preds)
    wrap_map(j, &blk)
  end

  # :nodoc
  def natjoin(rels, &blk)
    # for all pairs of relations, add predicates on matching column names
		preds = BudJoin::natural_preds(self, rels)
    j = join(rels, *preds, &blk)
  end

  def leftjoin(rels, *preds, &blk)
    j = BudLeftJoin.new(rels, self, preds)
    wrap_map(j, &blk)
  end

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
