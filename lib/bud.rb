require 'rubygems'
require 'eventmachine'
require 'msgpack'
require 'socket'
require 'superators'
require 'thread'

require 'bud/monkeypatch'

require 'bud/aggs'
require 'bud/bud_meta'
require 'bud/collections'
require 'bud/depanalysis'
require 'bud/deploy/forkdeploy'
require 'bud/deploy/threaddeploy'
require 'bud/errors'
require 'bud/joins'
require 'bud/rtrace'
require 'bud/server'
require 'bud/state'
require 'bud/storage/dbm'
require 'bud/storage/tokyocabinet'
require 'bud/storage/zookeeper'
require 'bud/stratify'
require 'bud/viz'

$em_stopped = Queue.new

# The root Bud module. To cause an instance of Bud to begin executing, there are
# three main options:
#
# 1. Synchronously. To do this, instantiate your program and then call tick()
#    one or more times; each call evaluates a single Bud timestep. Note that in
#    this mode, network communication (channels) and timers cannot be used. This
#    is mostly intended for "one-shot" programs that compute a single result and
#    then terminate.
# 2. In a separate thread in the foreground. To do this, instantiate your
#    program and then call run_fg(). The Bud interpreter will then run, handling
#    network events and evaluating new timesteps as appropriate. The run_fg()
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
  attr_reader :stratum_first_iter, :joinstate
  attr_accessor :lazy # This can be changed on-the-fly by REBL
  attr_accessor :stratum_collection_map

  # options to the bud runtime are passed in a hash, with the following keys
  # * network configuration
  #   * <tt>:ip</tt>   IP address string for this instance
  #   * <tt>:port</tt>   port number for this instance
  #   * <tt>:ext_ip</tt>  IP address at which external nodes can contact this instance
  #   * <tt>:ext_port</tt>   port number to go with :ext_ip
  #   * <tt>:bust_port</tt>  port number for the restful http messages
  # * operating system interaction
  #   * <tt>:stdin</tt>  if non-nil, reading from the +stdio+ collection results in reading from this +IO+ handle
  #   * <tt>:stdout</tt> writing to the +stdio+ collection results in writing to this +IO+ handle; defaults to +$stdout+
  #   * <tt>:no_signal_handlers</tt> if true, runtime ignores SIGINT and SIGTERM
  # * tracing and output
  #   * <tt>:quiet</tt> if true, suppress certain messages
  #   * <tt>:trace</tt> if true, generate budvis outputs
  #   * <tt>:rtrace</tt>  if true, generate budplot outputs
  #   * <tt>:dump_rewrite</tt> if true, dump results of internal rewriting of Bloom code to a file
  # * controlling execution 
  #   * <tt>:lazy</tt>  if true, prevents runtime from ticking except on external calls to +tick+
  #   * <tt>:tag</tt>  a name for this instance, suitable for display during tracing and visualization
  # * storage configuration
  #   * <tt>:tc_dir</tt>  filesystem directory to hold TokyoCabinet data stores
  #   * <tt>:tc_truncate</tt> if true, TokyoCabinet collections are opened with OTRUNC
  # * deployment
  #   * <tt>:deploy</tt>  enable deployment
  def initialize(options={})
    @tables = {}
    @table_meta = []
    @rewritten_strata = []
    @channels = {}
    @tc_tables = {}
    @dbm_tables = {}
    @zk_tables = {}
    @callbacks = {}
    @callback_id = 0
    @timers = []
    @budtime = 0
    @inbound = []
    @done_bootstrap = false
    @joinstate = {}  # joins are stateful, their state needs to be kept inside the Bud instance

    # Setup options (named arguments), along with default values
    @options = options
    @lazy = @options[:lazy] ||= false
    @options[:ip] ||= "127.0.0.1"
    @ip = @options[:ip]
    @options[:port] ||= 0
    @options[:port] = @options[:port].to_i
    # NB: If using an ephemeral port (specified by port = 0), the actual port
    # number won't be known until we start EM

    relatives = self.class.modules + [self.class]
    relatives.each do |r|
      Bud.rewrite_local_methods(r)
    end

    @declarations = ModuleRewriter.get_rule_defs(self.class)

    init_state

    @viz = VizOnline.new(self) if @options[:trace]
    @rtracer = RTrace.new(self) if @options[:rtrace]

    # Get dependency info and determine stratification order.
    unless self.class <= Stratification or self.class <= DepAnalysis
      do_rewrite
    end

    # Load the rules as a closure. Each element of @strata is an array of
    # lambdas, one for each rewritten rule in that strata. Note that legacy Bud
    # code (with user-specified stratification) assumes that @strata is a simple
    # array, so we need to convert it before loading the rewritten strata.
    @strata = []
    @rule_src = []
    @rule_orig_src = []
    declaration
    @strata.each_with_index do |s,i|
      raise BudError if s.class <= Array
      @strata[i] = [s]
      # Don't try to record source text for old-style rule blocks
      @rule_src[i] = [""]
    end

    @rewritten_strata.each_with_index do |src_ary,i|
      @strata[i] ||= []
      @rule_src[i] ||= []
      @rule_orig_src[i] ||= []
      src_ary.each_with_index do |src, j|
        @strata[i] << eval("lambda { #{src} }")
        @rule_src[i] << src
        @rule_orig_src[i] << @no_attr_rewrite_strata[i][j]
      end
    end
  end

  private

  # Rewrite methods defined in the given klass to expand module references and
  # temp collections. Imported modules are rewritten during the import process;
  # we rewrite the main Bud class and any included modules here. Note that we
  # only rewrite each distinct Class once.
  def self.rewrite_local_methods(klass)
    @done_rewrite ||= {}
    return if @done_rewrite.has_key? klass.name

    u = Unifier.new
    ref_expander = NestedRefRewriter.new(klass.bud_import_table)
    tmp_expander = TempExpander.new
    r2r = Ruby2Ruby.new

    klass.instance_methods(false).each do |m|
      ast = ParseTree.translate(klass, m)
      ast = u.process(ast)
      ast = ref_expander.process(ast)
      ast = tmp_expander.process(ast)

      if (ref_expander.did_work or tmp_expander.did_work)
        new_source = r2r.process(ast)
        klass.module_eval new_source # Replace previous method def
      end

      ref_expander.did_work = false
      tmp_expander.did_work = false
    end

    # If we found any temp statements in the klass's rule blocks, add a state
    # block with declarations for the corresponding temp collections.
    s = tmp_expander.get_state_meth(klass)
    if s
      state_src = r2r.process(s)
      klass.module_eval(state_src)
    end

    # Always rewrite anonymous classes
    @done_rewrite[klass.name] = true unless klass.name == ""
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
    @rewritten_strata, @no_attr_rewrite_strata = @meta_parser.meta_rewrite
  end

  public

  ########### give empty defaults for these
  def declaration # :nodoc: all
  end
  def bootstrap # :nodoc: all
  end

  ########### metaprogramming support for ruby and for rule rewriting
  # helper to define instance methods
  def singleton_class # :nodoc: all
    class << self; self; end
  end

  ######## methods for controlling execution

  # Run Bud in the background (in a different thread). This means that the Bud
  # interpreter will run asynchronously from the caller, so care must be used
  # when interacting with it. For example, it is not safe to directly examine
  # Bud collections from the caller's thread (see async_do and sync_do).
  #
  # This instance of Bud will continue to execute until stop_bg is called.
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
  # Note that run_fg cannot be invoked if run_bg has already been called in the
  # same Ruby process.
  #
  # Execution proceeds in time ticks, a la Dedalus.
  # * Within each tick there may be multiple strata.
  # * Within each stratum we do multiple semi-naive iterations.
  def run_fg
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
      $em_stopped.pop
    else
      schedule_and_wait do
        do_shutdown(false)
      end
    end
  end

  # Given a block, evaluate that block inside the background Ruby thread at some
  # time in the future. Because the block is evaluate inside the background Ruby
  # thread, the block can safely examine Bud state. Naturally, this method can
  # only be used when Bud is running in the background. Note that calling
  # sync_do blocks the caller until the block has been evaluated; for a
  # non-blocking version, see async_do.
  #
  # Note that the block is invoked after one Bud timestep has ended but before
  # the next timestep begins. Hence, synchronous accumulation (<=) into a Bud
  # scratch collection in a callback is typically not a useful thing to do: when
  # the next tick begins, the content of any scratch collections will be
  # emptied, which includes anything inserted by a sync_do block using <=. To
  # avoid this behavior, insert into scratches using <+.
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
  # after you're finished using Bud. Programs that use Bud via run_fg() or
  # run_bg() don't need to call this manually.
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

  # sync_callback supports synchronous interaction with Bud modules.  The caller
  # supplies the name of an input collection, a set of tuples to insert, and an
  # output collection on which to 'listen.'  The call blocks until tuples are
  # inserted into the output collection: these are returned to the caller.
  def sync_callback(in_tbl, tupleset, out_tbl)
    q = Queue.new
    cb = register_callback(out_tbl) do |c|
      q.push c.to_a
    end
    unless in_tbl.nil?
      sync_do {
        t = @tables[in_tbl]
        if t.class <= Bud::BudChannel or t.class <= Bud::BudZkTable
          t <~ tupleset
        else
          t <+ tupleset
        end
      }
    end
    result = q.pop
    unregister_callback(cb)
    return result
  end

  # A common special case for sync_callback: block on a delta to a table.
  def delta(out_tbl)
    sync_callback(nil, nil, out_tbl)
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
      $em_stopped << true
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
      Signal.trap("TERM") do
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
    @stdio.start_stdin_reader if @options[:stdin]
    @zk_tables.each_value {|t| t.start_watchers}

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
  # and port, the user may define an external IP and/or port. The external
  # version of each is returned if available.  If not, the local version is
  # returned.  There are use cases for mixing and matching local and external.
  # local_ip:external_port would be if you have local port forwarding, and
  # external_ip:local_port would be if you're in a DMZ, for example.
  def ip_port
    raise BudError, "ip_port called before port defined" if @port.nil? and @options[:port] == 0 and not @options[:ext_port]

    ip = options[:ext_ip] ? "#{@options[:ext_ip]}" : "#{@ip}"
    port = options[:ext_port] ? "#{@options[:ext_port]}" :
      (@port.nil? ? "#{@options[:port]}" : "#{@port}")
    ip + ":" + port
  end

  # Returns the internal IP and port.  See ip_port.
  def int_ip_port
    raise BudError, "ip_port called before port defined" if @port.nil? and @options[:port] == 0
    @port.nil? ? "#{@ip}:#{@options[:port]}" : "#{@ip}:#{@port}"
  end

  # Manually trigger one timestep of Bloom execution.
  def tick
    @tables.each_value do |t|
      t.tick
    end
    
    @joinstate = {}

    do_bootstrap unless @done_bootstrap
    receive_inbound

    @strata.each_with_index { |s,i| stratum_fixpoint(s, i) }
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
    table :t_rules, [:rule_id] => [:lhs, :op, :src, :orig_src]
    table :t_depends, [:rule_id, :lhs, :op, :body] => [:nm]
    table :t_depends_tc, [:head, :body, :via, :neg, :temporal]
    table :t_provides, [:interface] => [:input]
    table :t_underspecified, t_provides.schema
    table :t_stratum, [:predicate] => [:stratum]
    table :t_cycle, [:predicate, :via, :neg, :temporal]
    table :t_table_info, [:tab_name, :tab_type]
    table :t_table_schema, [:tab_name, :col_name, :ord, :loc]
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
    @dbm_tables.each_value { |t| t.flush }
  end

  def stratum_fixpoint(strat, strat_num)
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
      strat.each_with_index do |r,i|
        fixpoint = false
        begin
          r.call
        rescue Exception => e
          # Don't report source text for certain rules (old-style rule blocks)
          rule_src = @rule_orig_src[strat_num][i] unless @rule_orig_src[strat_num].nil?
          src_msg = ""
          unless rule_src == ""
            src_msg = "\nRule: #{rule_src}"
          end

          new_e = e
          unless new_e.class <= BudError
            new_e = BudError
          end
          raise new_e, "Exception during Bud evaluation.\nException: #{e.inspect}.#{src_msg}"
        end        
      end
      @stratum_first_iter = false
      fixpoint = true
      # tick collections in this stratum; if we don't have info on that, tick all collections
      colls = @stratum_collection_map[strat_num] if @stratum_collection_map
      colls ||= @tables.keys
      colls.each do |name|
        begin
          coll = self.send(name) 
          unless coll.delta.empty? and coll.new_delta.empty?
            coll.tick_deltas
            fixpoint = false
          end
        rescue
          # ignore missing tables; rebl for example deletes them mid-stream
        end
      end      
    end while not fixpoint
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
