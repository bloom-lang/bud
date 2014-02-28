require 'rubygems'
gem 'ruby2ruby', '>= 2.0.1'
gem 'ruby_parser', '>= 3.0.2'

require 'eventmachine'
require 'msgpack'
require 'ruby2ruby'
require 'ruby_parser'
require 'set'
require 'socket'
require 'superators19'
require 'thread'

require 'bud/errors'
require 'bud/monkeypatch'

require 'bud/aggs'
require 'bud/bud_meta'
require 'bud/collections'
require 'bud/executor/elements.rb'
require 'bud/executor/group.rb'
require 'bud/executor/join.rb'
require 'bud/lattice-core'
require 'bud/lattice-lib'
require 'bud/metrics'
require 'bud/rtrace'
require 'bud/server'
require 'bud/state'
require 'bud/storage/dbm'
require 'bud/storage/zookeeper'
require 'bud/viz'

ILLEGAL_INSTANCE_ID = -1
SIGNAL_CHECK_PERIOD = 0.2

$BUD_DEBUG = ENV["BUD_DEBUG"].to_i > 0
$BUD_SAFE = ENV["BUD_SAFE"].to_i > 0

$signal_lock = Mutex.new
$got_shutdown_signal = false
$signal_handler_setup = false
$instance_id = 0
$bud_instances = {}        # Map from instance id => Bud instance

# The root Bud module. To cause an instance of Bud to begin executing, there are
# three main options:
#
# 1. Synchronously. To do this, instantiate your program and then call tick()
#    one or more times; each call evaluates a single Bud timestep. In this mode,
#    any network messages or timer events that occur will be buffered until the
#    next call to tick(). This is mostly intended for "one-shot" programs that
#    compute a single result and then terminate, or for interactively
#    "single-stepping" through the execution of an event-driven system.
# 2. In a separate thread in the foreground. To do this, instantiate your
#    program and then call run_fg(). The Bud interpreter will then run, handling
#    network events and evaluating new timesteps as appropriate. The run_fg()
#    method will not return unless an error occurs.
# 3. In a separate thread in the background. To do this, instantiate your
#    program and then call run_bg(). The Bud interpreter will run
#    asynchronously. To interact with Bud (e.g., insert additional data or
#    inspect the state of a Bud collection), use the sync_do and async_do
#    methods.
#
# Most programs should use method #3. Note that in all three cases, the stop()
# method should be used to shutdown a Bud instance and release any resources it
# is using.
#
# :main: Bud
module Bud
  attr_reader :budtime, :inbound, :options, :meta_parser, :viz, :rtracer, :dsock
  attr_reader :tables, :builtin_tables, :channels, :zk_tables, :dbm_tables, :app_tables, :lattices
  attr_reader :push_sources, :push_elems, :push_joins, :scanners, :merge_targets
  attr_reader :this_stratum, :this_rule_context, :done_bootstrap
  attr_reader :inside_tick
  attr_accessor :stratified_rules
  attr_accessor :metrics, :periodics
  attr_accessor :qualified_name
  attr_reader :running_async

  # options to the Bud runtime are passed in a hash, with the following keys
  # * network configuration
  #   * <tt>:ip</tt>   IP address string for this instance
  #   * <tt>:port</tt>   port number for this instance
  #   * <tt>:ext_ip</tt>  IP address at which external nodes can contact this instance
  #   * <tt>:ext_port</tt>   port number to go with <tt>:ext_ip</tt>
  # * operating system interaction
  #   * <tt>:stdin</tt>  if non-nil, reading from the +stdio+ collection results in reading from this +IO+ handle
  #   * <tt>:stdout</tt> writing to the +stdio+ collection results in writing to this +IO+ handle; defaults to <tt>$stdout</tt>
  #   * <tt>:signal_handling</tt> how to handle +SIGINT+ and +SIGTERM+.  If +:none+, these signals are ignored.  Else shutdown all bud instances.
  # * tracing and output
  #   * <tt>:quiet</tt> if true, suppress certain messages
  #   * <tt>:trace</tt> if true, generate +budvis+ outputs
  #   * <tt>:rtrace</tt>  if true, generate +budplot+ outputs
  #   * <tt>:dump_rewrite</tt> if true, dump results of internal rewriting of Bloom code to a file
  #   * <tt>:print_wiring</tt> if true, print the wiring diagram of the program to stdout
  #   * <tt>:metrics</tt> if true, dumps a hash of internal performance metrics
  # * controlling execution
  #   * <tt>:tag</tt>  a name for this instance, suitable for display during tracing and visualization
  #   * <tt>:channel_filter</tt> a code block that can be used to filter the
  #     network messages delivered to this Bud instance. At the start of each
  #     tick, the code block is invoked for every channel with any incoming
  #     messages; the code block is passed the name of the channel and an array
  #     containing the inbound messages. It should return a two-element array
  #     containing "accepted" and "postponed" messages, respectively. Accepted
  #     messages are delivered during this tick, and postponed messages are
  #     buffered and passed to the filter in subsequent ticks. Any messages that
  #     aren't in either array are dropped.
  # * storage configuration
  #   * <tt>:dbm_dir</tt> filesystem directory to hold DBM-backed collections
  #   * <tt>:dbm_truncate</tt> if true, DBM-backed collections are opened with +OTRUNC+
  def initialize(options={})
    options[:dump_rewrite] ||= ENV["BUD_DUMP_REWRITE"].to_i > 0
    options[:dump_ast]     ||= ENV["BUD_DUMP_AST"].to_i > 0
    options[:print_wiring] ||= ENV["BUD_PRINT_WIRING"].to_i > 0
    @qualified_name = ""
    @tables = {}
    @lattices = {}
    @channels = {}
    @dbm_tables = {}
    @zk_tables = {}
    @stratified_rules = []
    @push_elems = {}
    @callbacks = {}
    @callback_id = 0
    @shutdown_callbacks = {}
    @shutdown_callback_id = 0
    @post_shutdown_callbacks = []
    @timers = []
    @app_tables = []
    @inside_tick = false
    @tick_clock_time = nil
    @budtime = 0
    @inbound = {}
    @done_bootstrap = false
    @done_wiring = false
    @instance_id = ILLEGAL_INSTANCE_ID # Assigned when we start running
    @metrics = {}
    @endtime = nil
    @this_stratum = -1
    @this_rule_id = -1
    @push_sorted_elems = nil
    @running_async = false
    @bud_started = false

    # Setup options (named arguments), along with default values
    @options = options.clone
    @options[:ip] ||= "127.0.0.1"
    @ip = @options[:ip]
    @options[:port] ||= 0
    @options[:port] = @options[:port].to_i
    # NB: If using an ephemeral port (specified by port = 0), the actual port
    # number won't be known until we start EM

    load_lattice_defs
    builtin_state
    resolve_imports
    call_state_methods

    @viz = VizOnline.new(self) if @options[:trace]
    @rtracer = RTrace.new(self) if @options[:rtrace]

    do_rewrite
    if toplevel == self
      # initialize per-stratum state
      @num_strata = @stratified_rules.length
      @scanners = @num_strata.times.map{{}}
      @push_sources = @num_strata.times.map{{}}
      @push_joins = @num_strata.times.map{[]}
      @merge_targets = @num_strata.times.map{Set.new}
    end
  end

  def module_wrapper_class(mod)
    class_name = "#{mod}__wrap"
    begin
      klass = Module.const_get(class_name.to_sym)
      unless klass.is_a? Class
        raise Bud::Error, "internal error: #{class_name} is in use"
      end
    rescue NameError # exception if class class_name doesn't exist
    end
    klass ||= eval "class #{class_name}; include Bud; include #{mod}; end"
    klass
  end

  def toplevel
     @toplevel = (@options[:toplevel] || self)
  end

  def qualified_name
    toplevel? ? "" : @options[:qualified_name]
  end

  def toplevel?
    toplevel.object_id == self.object_id
  end

  def import_instance(name)
    name = "@" + name.to_s
    instance_variable_get(name) if instance_variable_defined? name
  end

  def budtime
    toplevel? ? @budtime : toplevel.budtime
  end

  # Are we currently in the process of wiring together the dataflow?
  def wiring?
    toplevel? ? (@done_bootstrap && !@done_wiring) : toplevel.wiring?
  end

  private
  def import_defs
    @imported_defs = {} # mod name -> Module map
    self.class.ancestors.each do |anc|
      if anc.respond_to? :bud_import_table
        anc_imp_tbl = anc.bud_import_table
        anc_imp_tbl.each do |nm, mod|
          # Ensure that two modules have not been imported under one alias.
          if @imported_defs.member? nm and not @imported_defs[nm] == anc_imp_tbl[nm]
              raise Bud::CompileError, "conflicting imports: modules #{@imported_defs[nm]} and #{anc_imp_tbl[nm]} both are imported as '#{nm}'"
          end
          @imported_defs[nm] = mod
        end
      end
    end
    @imported_defs ||= self.class.ancestors.inject({}) {|tbl, e| tbl.merge(e.bud_import_table)}
  end

  # absorb rules and dependencies from imported modules. The corresponding module instantiations
  # would themselves have resolved their own imports.
  def resolve_imports
    import_tbl = import_defs

    import_tbl.each_pair do |local_name, mod_name|
      # corresponding to "import <mod_name> => :<local_name>"
      mod_inst = send(local_name)
      if mod_inst.nil?
        # create wrapper instances
        #puts "=== resolving #{self}.#{mod_name} => #{local_name}"
        klass = module_wrapper_class(mod_name)
        qlocal_name = toplevel? ? local_name.to_s : "#{self.qualified_name}.#{local_name}"
        # this instantiation will resolve the imported module's own imports
        mod_inst = klass.new(:toplevel => toplevel, :qualified_name => qlocal_name)
        instance_variable_set("@#{local_name}", mod_inst)
      end
      # Absorb the module wrapper's user-defined state.
      mod_inst.tables.each_pair do |name, t|
        # Don't try to import module definitions for builtin Bud state. Note
        # that @tables only includes the builtin tables, because resolve_imports
        # is called before user-defined state blocks are run.
        unless @tables.has_key? t.tabname
          qname = "#{local_name}.#{name}"
          tables[qname.to_sym] = t
        end
      end
      mod_inst.lattices.each_pair do |name, t|
        qname = "#{local_name}.#{name}".to_sym
        raise Bud::Error if lattices.has_key? qname
        lattices[qname] = t
      end
      mod_inst.t_rules.each do |imp_rule|
        qname = "#{local_name}.#{imp_rule.lhs}"
        self.t_rules << [imp_rule.bud_obj, imp_rule.rule_id, qname, imp_rule.op,
                         imp_rule.src, imp_rule.orig_src, imp_rule.unsafe_funcs_called]
      end
      mod_inst.t_depends.each do |imp_dep|
        qlname = "#{local_name}.#{imp_dep.lhs}"
        qrname = "#{local_name}.#{imp_dep.body}"
        self.t_depends << [imp_dep.bud_obj, imp_dep.rule_id, qlname,
                           imp_dep.op, qrname, imp_dep.nm, imp_dep.in_body]
      end
      mod_inst.t_provides.each do |imp_pro|
        qintname = "#{local_name}.#{imp_pro.interface}"
        self.t_provides << [qintname, imp_pro.input]
      end
      mod_inst.channels.each do |name, ch|
        qname = "#{local_name}.#{name}"
        @channels[qname.to_sym] = ch
      end
      mod_inst.dbm_tables.each do |name, t|
        qname = "#{local_name}.#{name}"
        @dbm_tables[qname.to_sym] = t
      end
      mod_inst.periodics.each do |p|
        qname = "#{local_name}.#{p.pername}"
        @periodics << [qname.to_sym, p.period]
      end
    end

    nil
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

  def do_wiring
    @stratified_rules.each_with_index { |rules, stratum| eval_rules(rules, stratum) }

    # Prepare list of tables that will be actively used at run time. First, all
    # the user-defined tables and lattices.  We start @app_tables off as a set,
    # then convert to an array later.
    @app_tables = (@tables.keys - @builtin_tables.keys).map {|t| @tables[t]}.to_set
    @app_tables.merge(@lattices.values)

    # Check scan and merge_targets to see if any builtin_tables need to be added as well.
    @scanners.each do |scs|
      @app_tables.merge(scs.values.map {|s| s.collection})
    end
    @merge_targets.each do |mts| #mts == merge_targets at stratum
      @app_tables.merge(mts)
    end
    @app_tables = @app_tables.to_a

    # for each stratum create a sorted list of push elements in topological order
    @push_sorted_elems = []
    @scanners.each do |scs|  # scs's values constitute scanners at a stratum
      # start with scanners and transitively add all reachable elements in a
      # breadth-first order
      working = scs.values
      seen = Set.new(working)
      sorted_elems = [] # sorted elements in this stratum
      while not working.empty?
        sorted_elems.concat(working)
        wired_to = []
        working.each do |e|
          e.wirings.each do |out|
            if ((out.class <= PushElement || out.class <= LatticePushElement) and not seen.member?(out))
              seen << out
              wired_to << out
            end
          end
        end
        working = wired_to
      end
      @push_sorted_elems << sorted_elems
    end

    @merge_targets.each_with_index do |stratum_targets, stratum|
      @scanners[stratum].each_value do |s|
        stratum_targets << s.collection
      end
    end

    # We create "orphan" scanners for collections that don't appear on the RHS
    # of any rules, but do appear on the LHS of at least one rule. These
    # scanners aren't needed to compute the fixpoint, but they are used as part
    # of rescan/invalidation (e.g., if an orphaned collection receives a manual
    # deletion operation, we need to arrange for the collection to be
    # re-filled).
    @orphan_scanners = []       # Pairs of [scanner, stratum]
    @app_tables.each do |t|
      next unless t.class <= Bud::BudCollection         # skip lattice wrappers
      next if t.scanner_cnt > 0

      stratum = collection_stratum(t.qualified_tabname.to_s)
      # if the collection also doesn't appear on any LHSs, skip it
      next if stratum.nil?
      @orphan_scanners << [Bud::ScannerElement.new(t.tabname, self, t, t.schema),
                           stratum]
    end

    # Sanity check
    @push_sorted_elems.each do |stratum_elems|
      stratum_elems.each {|se| se.check_wiring}
    end

    # Create sets of elements and collections to invalidate or rescan at the
    # beginning of each tick
    prepare_invalidation_scheme

    # For all tables that are accessed (scanned) in a stratum higher than the
    # one they are updated in, set a flag to track deltas accumulated in that
    # tick (see: collection.tick_delta)
    stratum_accessed = {}
    (@num_strata-1).downto(0) do |stratum|
      @scanners[stratum].each_value do |s|
        stratum_accessed[s.collection] ||= stratum
      end
    end
    @merge_targets.each_with_index do |stratum_targets, stratum|
      stratum_targets.each {|tab|
        tab.accumulate_tick_deltas = true if stratum_accessed[tab] and stratum_accessed[tab] > stratum
      }
    end

    @done_wiring = true
    if @options[:print_wiring]
      @push_sources.each do |strat|
        strat.each_value {|src| src.print_wiring}
      end
    end
  end

  # All collections (elements included) are semantically required to erase any
  # cached information at the start of a tick and start from a clean
  # slate. prepare_invalidation_scheme prepares a just-in-time invalidation
  # scheme that permits us to preserve data from one tick to the next, and to
  # keep things in incremental mode unless there's a negation.
  #
  # This scheme solves the following constraints.
  #
  # 1. A full scan of an element's contents results in downstream elements
  #    getting full scans themselves (i.e., no deltas). This effect is
  #    transitive.
  # 2. Invalidation of an element's cache results in rebuilding of the cache and
  #    a consequent fullscan. See next.
  # 3. Invalidation of an element requires upstream elements to rescan their
  #    contents, or to transitively pass the request on further upstream. Any
  #    element that has a cache can rescan without passing on the request to
  #    higher levels.
  #
  # This set of constraints is solved once during wiring, resulting in four data
  # structures:
  #
  # @default_invalidate = Set of elements and tables to always invalidate at
  #                       every tick.
  #
  # @default_rescan = Set of elements and tables to always scan fully in the
  #                   first iteration of every tick.
  #
  # scanner[stratum].invalidate_set = Set of elements to additionally invalidate
  #                                   if the scanner's table is invalidated at
  #                                   run-time.
  #
  # scanner[stratum].rescan_set = Similar to above.
  def prepare_invalidation_scheme
    if $BUD_SAFE
      @app_tables = @tables.values + @lattices.values # No collections excluded

      rescan = Set.new
      invalidate = @app_tables.select {|t| t.class <= BudScratch}.to_set
      @num_strata.times do |stratum|
        @push_sorted_elems[stratum].each do |elem|
          invalidate << elem
          rescan << elem
        end
      end
      @default_rescan = rescan.to_a
      @default_invalidate = invalidate.to_a
      @reset_list = [] # Nothing to reset at end of tick. It'll be overwritten anyway
      return
    end

    # By default, all tables are considered sources unless they appear on the
    # lhs.  We only consider non-temporal rules because invalidation is only
    # about this tick.  Also, we track (in unsafe_targets) those tables that are
    # the targets of user-defined code blocks that call "unsafe" functions that
    # produce a different value in every tick (e.g., budtime). Elements that
    # feed these tables are forced to rescan their contents, and thus forced to
    # re-execute these code blocks.
    unsafe_targets = Set.new
    t_rules.each do |rule|
      lhs = rule.lhs.to_sym
      if rule.op == "<="
        # Note that lattices cannot be sources
        @tables[lhs].is_source = false if @tables.has_key? lhs
      end
      unsafe_targets << lhs if rule.unsafe_funcs_called
    end

    # Compute a set of tables and elements that should be explicitly told to
    # invalidate or rescan.  Start with a set of tables that always invalidate
    # and elements that always rescan.
    invalidate = @app_tables.select {|t| t.invalidate_at_tick}.to_set
    rescan = Set.new

    @num_strata.times do |stratum|
      @push_sorted_elems[stratum].each do |elem|
        rescan << elem if elem.rescan_at_tick

        if elem.outputs.any?{|tab| not(tab.class <= PushElement) and not(tab.class <= LatticePushElement) and unsafe_targets.member? tab.qualified_tabname.to_sym }
          rescan.merge(elem.wired_by)
        end
      end
      rescan_invalidate_tc(stratum, rescan, invalidate)
    end

    @default_rescan = rescan.to_a
    @default_invalidate = invalidate.to_a

    if $BUD_DEBUG
      puts "Default rescan: #{rescan.inspect}"
      puts "Default inval: #{invalidate.inspect}"
      puts "Unsafe targets: #{unsafe_targets.inspect}"
    end

    # For each collection that is to be scanned, compute the set of dependent
    # tables and elements that will need invalidation and/or rescan if that
    # table were to be invalidated at runtime.
    dflt_rescan = rescan
    dflt_invalidate = invalidate
    to_reset = rescan + invalidate
    each_scanner do |scanner, stratum|
      # If it is going to be always invalidated, it doesn't need further
      # examination. Lattice scanners also don't get invalidated.
      next if dflt_rescan.member? scanner
      next if scanner.class <= LatticeScanner

      rescan = dflt_rescan.clone
      invalidate = dflt_invalidate + [scanner.collection]
      rescan_invalidate_tc(stratum, rescan, invalidate)
      prune_rescan_set(rescan)

      # Make sure we reset the rescan/invalidate flag for this scanner at
      # end-of-tick, but we can remove the scanner from its own
      # rescan_set/inval_set.
      to_reset.merge(rescan)
      to_reset.merge(invalidate)
      rescan.delete(scanner)
      invalidate.delete(scanner.collection)

      # Give the diffs (from default) to scanner; these are elements that are
      # dependent on this scanner
      diffscan = (rescan - dflt_rescan).find_all {|elem| elem.class <= PushElement}
      scanner.invalidate_at_tick(diffscan, (invalidate - dflt_invalidate).to_a)
    end
    @reset_list = to_reset.to_a

    # For each lattice, find the collections that should be rescanned when there
    # is a new delta for the lattice. That is, if we have a rule like:
    # "t2 <= t1 {|t| [t.key, lat_foo]}", whenever there is a delta on lat_foo we
    # should rescan t1 (to produce tuples with the updated lat_foo value).
    #
    # TODO:
    # (1) if t1 is fed by rules r1 and r2 but only r1 references lattice x,
    #     don't trigger rescan of r2 on deltas for x (hard)
    t_depends.each do |dep|
      src, target_name = dep.body.to_sym, dep.lhs.to_sym
      if @lattices.has_key? src and dep.in_body
        src_lat = @lattices[src]
        if @tables.has_key? target_name
          target = @tables[target_name]
        else
          target = @lattices[target_name]
        end

        # Conservatively, we rescan all the elements that feed the lhs (target)
        # collection via positive (non-deletion) rules; we then also need to
        # potentially rescan ancestors of those elements as well (e.g., setting
        # a stateless PushElement to rescan does nothing; we want to tell its
        # ancestor ScannerElement to rescan).
        #
        # XXX: do we need to consider all transitively reachable nodes for
        # rescan?
        lat_rescan = target.positive_predecessors.to_set
        lat_inval = Set.new
        target.positive_predecessors.each do |e|
          e.add_rescan_invalidate(lat_rescan, lat_inval)
        end
        src_lat.rescan_on_delta.merge(lat_rescan)
      end
    end
  end

  # Given rescan, invalidate sets, compute transitive closure
  def rescan_invalidate_tc(stratum, rescan, invalidate)
    # XXX: hack. If there's nothing in the given stratum, don't do
    # anything. This can arise if we have an orphan scanner whose input is a
    # non-monotonic operator; the stratum(LHS) = stratum(RHS) + 1, but there's
    # nothing else in stratum(LHS).
    return if @push_sorted_elems[stratum].nil?

    rescan_len = rescan.size
    invalidate_len = invalidate.size
    while true
      # Ask each element if it wants to add itself to either set, depending on
      # who else is in those sets already.
      @push_sorted_elems[stratum].each {|t| t.add_rescan_invalidate(rescan, invalidate)}
      break if rescan_len == rescan.size and invalidate_len == invalidate.size
      rescan_len = rescan.size
      invalidate_len = invalidate.size
    end
  end

  def prune_rescan_set(rescan)
    rescan.delete_if {|e| e.rescan_at_tick}
  end

  def each_scanner
    @num_strata.times do |stratum|
      @scanners[stratum].each_value do |scanner|
        yield scanner, stratum
      end
    end

    @orphan_scanners.each do |scanner,stratum|
      yield scanner, stratum
    end
  end

  def do_rewrite
    @meta_parser = BudMeta.new(self)
    @stratified_rules = @meta_parser.meta_rewrite
  end

  public

  ########### give empty defaults for these
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
  # This instance of Bud will run until stop() is called.
  def run_bg
    start

    schedule_and_wait do
      if @running_async
        raise Bud::Error, "run_bg called on already-running Bud instance"
      end
      @running_async = true

      # Consume any events received while we weren't running async
      tick_internal
    end

    @rtracer.sleep if options[:rtrace]
  end

  # Startup a Bud instance if it is not currently started. This starts
  # EventMachine (if needed) and binds to a UDP server socket. If do_tick is
  # true, we also execute a single Bloom timestep. Regardless, calling this
  # method does NOT cause Bud to begin running asynchronously (see run_bg).
  def start(do_tick=false)
    start_reactor
    schedule_and_wait do
      do_startup unless @bud_started
      tick_internal if do_tick
    end
  end

  private
  def do_startup
    raise Bud::Error, "EventMachine not started" unless EventMachine::reactor_running?
    raise Bud::Error unless EventMachine::reactor_thread?

    @instance_id = Bud.init_signal_handlers(self)
    do_start_server
    @bud_started = true

    # Initialize periodics
    @periodics.each do |p|
      @timers << make_periodic_timer(p.pername, p.period)
    end

    # Arrange for Bud to read from stdin if enabled. Note that we can't do this
    # earlier because we need to wait for EventMachine startup.
    @stdio.start_stdin_reader if @options[:stdin]
    @zk_tables.each_value {|t| t.start_watchers}

    @halt_cb = register_callback(:halt) do |t|
      stop
      if t.first.key == :kill
        Bud.shutdown_all_instances
        Bud.stop_em_loop
      end
    end
  end

  # Pause an instance of Bud that is running asynchronously. That is, this
  # method allows a Bud instance operating in run_bg or run_fg mode to be
  # switched to "single-stepped" mode; timesteps can be manually invoked via
  # tick(). To switch back to running Bud asynchronously, call run_bg().
  public
  def pause
    schedule_and_wait do
      @running_async = false
    end
  end

  # Run Bud in the "foreground" -- the caller's thread will be used to run the
  # Bud interpreter. This means this method won't return unless an error occurs
  # or Bud is halted. It is often more useful to run Bud in a different thread:
  # see run_bg.
  def run_fg
    # If we're called from the EventMachine thread (and EM is running), blocking
    # the current thread would imply deadlocking ourselves.
    if Thread.current == EventMachine::reactor_thread and EventMachine::reactor_running?
      raise Bud::Error, "cannot invoke run_fg from inside EventMachine"
    end

    q = Queue.new
    # Note that this must be a post-shutdown callback: if this is the only
    # thread, then the program might exit after run_fg() returns. If run_fg()
    # blocked on a normal shutdown callback, the program might exit before the
    # other shutdown callbacks have a chance to run.
    post_shutdown do
      q.push(true)
    end

    run_bg
    # Block caller's thread until Bud has shutdown
    q.pop
    report_metrics if options[:metrics]
  end

  # Shutdown a Bud instance and release any resources that it was using. This
  # method blocks until Bud has been shutdown. If +stop_em+ is true, the
  # EventMachine event loop is also shutdown; this will interfere with the
  # execution of any other Bud instances in the same process (as well as
  # anything else that happens to use EventMachine). We always shutdown the EM
  # loop if there are no more running Bud instances (this does interfere with
  # other EM-using apps, but it is necessary).
  def stop(stop_em=false, do_shutdown_cb=true)
    schedule_and_wait do
      do_shutdown(do_shutdown_cb)
    end

    # If we're shutting down the last active Bud instance, shutdown the EM event
    # loop as well. This is probably good practice in general, but it also
    # prevents weird EM behavior -- it seems as though EM::ConnectionNotBound
    # exceptions can be raised if the EM event loop is left running and
    # subsequent events arrive.
    $signal_lock.synchronize {
      stop_em = true if $bud_instances.empty? and EventMachine::reactor_running?
    }

    if stop_em
      Bud.stop_em_loop
      unless Thread.current == EventMachine::reactor_thread
        EventMachine::reactor_thread.join
      end
    end

    report_metrics if options[:metrics]
  end
  alias :stop_bg :stop

  # Register a callback that will be invoked when this instance of Bud is
  # shutting down.
  # XXX: The naming of this method (and cancel_shutdown_cb) is inconsistent
  # with the naming of register_callback and friends.
  def on_shutdown(&blk)
    # Start EM if not yet started
    start_reactor
    rv = nil
    schedule_and_wait do
      rv = @shutdown_callback_id
      @shutdown_callbacks[@shutdown_callback_id] = blk
      @shutdown_callback_id += 1
    end
    return rv
  end

  def cancel_shutdown_cb(id)
    schedule_and_wait do
      raise Bud::Error unless @shutdown_callbacks.has_key? id
      @shutdown_callbacks.delete(id)
    end
  end

  # Register a callback that will be invoked when *after* this instance of Bud
  # has been shutdown.
  def post_shutdown(&blk)
    # Start EM if not yet started
    start_reactor
    schedule_and_wait do
      @post_shutdown_callbacks << blk
    end
  end

  # Given a block, evaluate that block inside the background Ruby thread at some
  # time in the future, and then perform a Bloom tick. Because the block is
  # evaluate inside the background Ruby thread, the block can safely examine Bud
  # state. Note that calling sync_do blocks the caller until the block has been
  # evaluated; for a non-blocking version, see async_do.
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
      tick_internal
    end
  end

  # Like sync_do, but does not block the caller's thread: the given callback
  # will be invoked at some future time. Note that calls to async_do respect
  # FIFO order.
  def async_do
    EventMachine::schedule do
      yield if block_given?
      # Do another tick, in case the user-supplied block inserted any data
      tick_internal
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
  # Note that registering callbacks on persistent collections (e.g., tables,
  # syncs and stores) is probably not wise: as long as any tuples are stored in
  # the collection, the callback will be invoked at the end of every tick.
  def register_callback(tbl_name, &blk)
    # We allow callbacks to be added before or after EM has been started. To
    # simplify matters, we start EM if it hasn't been started yet.
    start_reactor
    cb_id = nil
    schedule_and_wait do
      cb_id = do_register_callback(tbl_name, &blk)
    end
    return cb_id
  end

  def do_register_callback(tbl_name, &blk)
    unless @tables.has_key? tbl_name
      raise Bud::Error, "no such collection: #{tbl_name}"
    end

    raise Bud::Error if @callbacks.has_key? @callback_id
    @callbacks[@callback_id] = [tbl_name, blk]
    cb_id = @callback_id
    @callback_id += 1
    return cb_id
  end

  # Unregister the callback that has the given ID.
  def unregister_callback(id)
    schedule_and_wait do
      raise Bud::Error, "missing callback: #{id.inspect}" unless @callbacks.has_key? id
      @callbacks.delete(id)
    end
  end

  # sync_callback supports synchronous interaction with Bud modules.  The caller
  # supplies the name of an input collection, a set of tuples to insert, and an
  # output collection on which to 'listen.'  The call blocks until tuples are
  # inserted into the output collection: these are returned to the caller.
  def sync_callback(in_tbl, tupleset, out_tbl)
    q = Queue.new

    # If the runtime shuts down before we see anything in the output collection,
    # make sure we hear about it so we can raise an error
    # XXX: Using two separate callbacks here is ugly.
    shutdown_cb = on_shutdown do
      q.push :callback
    end

    cb = nil
    sync_do {
      if in_tbl
        t = @tables[in_tbl]
        if t.class <= Bud::BudChannel or t.class <= Bud::BudZkTable
          t <~ tupleset
        else
          t <+ tupleset
        end
      end

      cb = do_register_callback(out_tbl) do |c|
        q.push c.to_a
      end
    }
    result = q.pop
    if result == :callback
      # Don't try to unregister the callbacks first: runtime is already shutdown
      raise Bud::ShutdownWithCallbacksError, "Bud instance shutdown before sync_callback completed"
    end
    unregister_callback(cb)
    cancel_shutdown_cb(shutdown_cb)
    return result
  end

  # A common special case for sync_callback: block on a delta to a table.
  def delta(out_tbl)
    sync_callback(nil, nil, out_tbl)
  end

  # XXX make tag specific
  def inspect
    "#{self.class}:#{self.object_id.to_s(16)}"
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
      # Only print a backtrace if a non-Bud::Error is raised (this presumably
      # indicates an unexpected failure).
      if e.class <= Bud::Error
        puts "#{e.class}: #{e}"
      else
        puts "Unexpected Bud error: #{e.inspect}"
        puts e.backtrace.join("\n")
      end
      Bud.shutdown_all_instances
      raise e
    end

    # Block until EM has successfully started up.
    q = Queue.new
    # This thread helps us avoid race conditions on the start and stop of
    # EventMachine's event loop.
    Thread.new do
      EventMachine.run do
        q.push(true)
      end
    end
    # Block waiting for EM's event loop to start up.
    q.pop
  end

  # Schedule a block to be evaluated by EventMachine in the future, and
  # block until this has happened.
  def schedule_and_wait
    # If EM isn't running, just run the user's block immediately
    # XXX: not clear that this is the right behavior
    unless EventMachine::reactor_running?
      yield
      return
    end

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

  def do_shutdown(do_shutdown_cb=true)
    # Silently ignore duplicate shutdown requests or attempts to shutdown an
    # instance that hasn't been started yet.
    return if @instance_id == ILLEGAL_INSTANCE_ID
    $signal_lock.synchronize {
      raise Bud::Error unless $bud_instances.has_key? @instance_id
      $bud_instances.delete @instance_id
      @instance_id = ILLEGAL_INSTANCE_ID
    }

    unregister_callback(@halt_cb)
    if do_shutdown_cb
      @shutdown_callbacks.each_value {|cb| cb.call}
    end
    @timers.each {|t| t.cancel}
    @tables.each_value {|t| t.close}
    if EventMachine::reactor_running? and @bud_started
      @dsock.close_connection
    end
    @bud_started = false
    @running_async = false
    if do_shutdown_cb
      @post_shutdown_callbacks.each {|cb| cb.call}
    end
  end

  def do_start_server
    @dsock = EventMachine::open_datagram_socket(@ip, @options[:port],
                                                BudServer, self,
                                                @options[:channel_filter])
    @port = Socket.unpack_sockaddr_in(@dsock.get_sockname)[0]
  end

  public

  # Returns the IP and port of the Bud instance as a string.  In addition to the
  # local IP and port, the user may define an external IP and/or port. The
  # external version of each is returned if available.  If not, the local
  # version is returned.  There are use cases for mixing and matching local and
  # external.  local_ip:external_port would be if you have local port
  # forwarding, and external_ip:local_port would be if you're in a DMZ, for
  # example.
  def ip_port
    raise Bud::Error, "ip_port called before port defined" if port.nil?
    ip.to_s + ":" + port.to_s
  end

  def ip
    options[:ext_ip] ? "#{@options[:ext_ip]}" : "#{@ip}"
  end

  def port
    return nil if @port.nil? and @options[:port] == 0 and not @options[:ext_port]
    return @options[:ext_port] ? @options[:ext_port] :
      (@port.nil? ? @options[:port] : @port)
  end

  # Returns the internal IP and port.  See ip_port.
  def int_ip_port
    raise Bud::Error, "int_ip_port called before port defined" if @port.nil? and @options[:port] == 0
    @port.nil? ? "#{@ip}:#{@options[:port]}" : "#{@ip}:#{@port}"
  end

  # From client code, manually trigger a timestep of Bloom execution.
  def tick
    start(true)
  end

  # Evaluate all bootstrap blocks and tick deltas
  def do_bootstrap
    # Evaluate bootstrap for imported modules
    @this_rule_context = self
    imported = import_defs.keys
    imported.each do |mod_alias|
      wrapper = import_instance mod_alias
      wrapper.do_bootstrap
    end
    self.class.ancestors.reverse.each do |anc|
      anc.instance_methods(false).each do |m|
        if /^__bootstrap__/.match m
          self.method(m.to_sym).call
        end
      end
    end
    bootstrap

    if toplevel == self
      @tables.each_value {|t| t.bootstrap}
      @lattices.each_value {|l| l.bootstrap}
    end
    @done_bootstrap = true
  end

  def do_invalidate_rescan
    @default_rescan.each {|elem| elem.rescan = true}
    @default_invalidate.each {|elem|
      elem.invalidated = true
      # Call tick on tables here itself. The rest below
      elem.invalidate_cache unless elem.class <= PushElement
    }

    # The following loop invalidates additional (non-default) elements and
    # tables that depend on the run-time invalidation state of a table.  Loop
    # once to set the flags.
    each_scanner do |scanner, stratum|
      if scanner.rescan
        scanner.rescan_set.each {|e| e.rescan = true}
        scanner.invalidate_set.each {|e|
          e.invalidated = true
          e.invalidate_cache unless e.class <= PushElement
        }
      end
    end

    # Loop a second time to actually call invalidate_cache.  We can't merge this
    # with the loops above because some versions of invalidate_cache (e.g.,
    # join) depend on the rescan state of other elements.
    @num_strata.times do |stratum|
      @push_sorted_elems[stratum].each {|e| e.invalidate_cache if e.invalidated}
    end
  end

  # One timestep of Bloom execution. This MUST be invoked from the EventMachine
  # thread; it is not intended to be called directly by client code.
  def tick_internal
    puts "#{object_id}/#{port} : ============================================= (#{@budtime})" if $BUD_DEBUG
    begin
      starttime = Time.now if options[:metrics]
      if options[:metrics] and not @endtime.nil?
        @metrics[:betweentickstats] ||= initialize_stats
        @metrics[:betweentickstats] = running_stats(@metrics[:betweentickstats],
                                                    starttime - @endtime)
      end
      @inside_tick = true

      unless @done_bootstrap
        do_bootstrap
        do_wiring
      else
        # inform tables and elements about beginning of tick.
        @app_tables.each {|t| t.tick}
        do_invalidate_rescan
      end

      receive_inbound
      # compute fixpoint for each stratum in order
      @stratified_rules.each_with_index do |rules,stratum|
        fixpoint = false
        first_iter = true
        until fixpoint
          @scanners[stratum].each_value {|s| s.scan(first_iter)}
          fixpoint = true
          first_iter = false
          # flush any tuples in the pipes
          @push_sorted_elems[stratum].each {|p| p.flush}
          # tick deltas on any merge targets and look for more deltas
          # check to see if any joins saw a delta
          @push_joins[stratum].each do |p|
            if p.found_delta
              fixpoint = false
              p.tick_deltas
            end
          end
          @merge_targets[stratum].each do |t|
            fixpoint = false if t.tick_deltas
          end
        end
        # push end-of-fixpoint
        @push_sorted_elems[stratum].each do |p|
          p.stratum_end
        end
        @merge_targets[stratum].each do |t|
          t.flush_deltas
        end
      end
      @viz.do_cards if @options[:trace]
      do_flush

      invoke_callbacks
      @budtime += 1
      @inbound.clear
      @reset_list.each {|e| e.invalidated = false; e.rescan = false}

    ensure
      @inside_tick = false
      @tick_clock_time = nil
    end

    if options[:metrics]
      @endtime = Time.now
      @metrics[:tickstats] ||= initialize_stats
      @metrics[:tickstats] = running_stats(@metrics[:tickstats], @endtime - starttime)
    end
  end

  # Returns the wallclock time associated with the current Bud tick. That is,
  # this value is guaranteed to remain the same for the duration of a single
  # tick, but will likely change between ticks.
  def bud_clock
    raise Bud::Error, "bud_clock undefined outside tick" unless @inside_tick
    @tick_clock_time ||= Time.now
    @tick_clock_time
  end

  # Return the stratum number of the given collection.
  # NB: if a collection does not appear on the lhs or rhs of any rules, it is
  # not currently assigned to a strata.
  def collection_stratum(collection)
    t_stratum.each do |t|
      return t.stratum if t.predicate == collection
    end

    return nil
  end

  private

  # Builtin Bud state (predefined collections). We could define this using the
  # standard state block syntax, but we want to ensure that builtin state is
  # initialized before user-defined state.
  def builtin_state
    # We expect there to be no previously-defined tables
    raise Bud::Error unless @tables.empty?

    loopback  :localtick, [:col1]
    @stdio = terminal :stdio
    scratch :halt, [:key]
    @periodics = table :periodics_tbl, [:pername] => [:period]

    # for BUD reflection
    table :t_cycle, [:predicate, :via, :neg, :temporal]
    table :t_depends, [:bud_obj, :rule_id, :lhs, :op, :body] => [:nm, :in_body]
    table :t_provides, [:interface] => [:input]
    table :t_rule_stratum, [:bud_obj, :rule_id] => [:stratum]
    table :t_rules, [:bud_obj, :rule_id] => [:lhs, :op, :src, :orig_src, :unsafe_funcs_called]
    table :t_stratum, [:predicate] => [:stratum]
    table :t_table_info, [:tab_name, :tab_type]
    table :t_table_schema, [:tab_name, :col_name, :ord, :loc]
    table :t_underspecified, t_provides.schema

    # Identify builtin tables as such
    @builtin_tables = @tables.clone if toplevel
  end

  # Handle external inputs: channels, terminals, and periodics. Received
  # messages are placed directly into the storage of the appropriate local
  # collection. The inbound queue is cleared at the end of the tick.
  def receive_inbound
    @inbound.each do |tbl_name, msg_buf|
      puts "recv via #{tbl_name}: #{msg_buf}" if $BUD_DEBUG
      msg_buf.each do |b|
        tables[tbl_name] << b
      end
    end
  end

  # "Flush" any tuples that need to be flushed. This does two things:
  # 1. Emit outgoing tuples in channels and ZK tables.
  # 2. Commit to disk any changes made to on-disk tables.
  def do_flush
    @channels.each_value { |c| c.flush }
    @zk_tables.each_value { |t| t.flush }
    @dbm_tables.each_value { |t| t.flush }
  end

  def eval_rule(__obj__, __src__)
    __obj__.instance_eval __src__  # ensure that the only local variables are __obj__ and __src__
  end

  def eval_rules(rules, strat_num)
    # This routine evals the rules in a given stratum, which results in a wiring
    # of PushElements
    @this_stratum = strat_num
    rules.each_with_index do |rule, i|
      # user-supplied code blocks will be evaluated in this context at run-time
      @this_rule_context = rule.bud_obj
      begin
        eval_rule(rule.bud_obj, rule.src)
      rescue Exception => e
        err_msg = "** Exception while wiring rule: #{rule.orig_src}\n ****** #{e}"
        # Create a new exception for accomodating err_msg, but reuse original backtrace
        new_e = (e.class <= Bud::Error) ? e.class.new(err_msg) : Bud::Error.new(err_msg)
        new_e.set_backtrace(e.backtrace)
        raise new_e
      end
    end
    @this_rule_context = nil
    @this_stratum = -1
  end

  ######## ids and timers
  def gen_id
    Time.new.to_i.to_s << rand.to_s
  end

  def make_periodic_timer(name, period)
    EventMachine::PeriodicTimer.new(period) do
      @inbound[name.to_sym] ||= []
      @inbound[name.to_sym] << [gen_id, Time.now]
      tick_internal if @running_async
    end
  end

  # Fork a new process. This is identical to Kernel#fork, except that it also
  # cleans up Bud and EventMachine-related state. As with Kernel#fork, the
  # caller supplies a code block that is run in the child process; the PID of
  # the child is returned by this method.
  def self.do_fork
    Kernel.fork do
      srand
      # This is somewhat grotty: we basically clone what EM::fork_reactor does,
      # except that we don't want the user-supplied block to be invoked by the
      # reactor thread.
      if EventMachine::reactor_running?
        EventMachine::stop_event_loop
        EventMachine::release_machine
        EventMachine::instance_variable_set('@reactor_running', false)
      end

      # Shutdown all the Bud instances inherited from the parent process, but
      # don't invoke their shutdown callbacks
      Bud.shutdown_all_instances(false)
      $got_shutdown_signal = false
      $signal_handler_setup = false

      yield
    end
  end

  # Note that this affects anyone else in the same process who happens to be
  # using EventMachine! This is also a non-blocking call; to block until EM
  # has completely shutdown, join on EM::reactor_thread.
  def self.stop_em_loop
    EventMachine::stop_event_loop

    # If another instance of Bud is started later, we'll need to reinitialize
    # the signal handlers (since they depend on EM).
    $signal_handler_setup = false
  end

  # Signal handling. If multiple Bud instances are running inside a single
  # process, we want a SIGINT or SIGTERM signal to cleanly shutdown all of
  # them. Note that we don't try to do any significant work in the signal
  # handlers themselves: we just set a flag that is checked by a periodic timer.
  def self.init_signal_handlers(b)
    $signal_lock.synchronize {
      # Initialize or re-initialize signal handlers if necessary.
      unless b.options[:signal_handling] == :none || $signal_handler_setup
        EventMachine::PeriodicTimer.new(SIGNAL_CHECK_PERIOD) do
          if $got_shutdown_signal
            Bud.shutdown_all_instances
            $got_shutdown_signal = false
          end
        end

        ["INT", "TERM"].each do |signal|
          Signal.trap(signal) {
            $got_shutdown_signal = true
          }
        end
        $signal_handler_setup = true
      end

      $instance_id += 1
      $bud_instances[$instance_id] = b
      return $instance_id
    }
  end

  def self.shutdown_all_instances(do_shutdown_cb=true)
    instances = nil
    $signal_lock.synchronize {
      instances = $bud_instances.clone
    }

    instances.each_value {|b| b.stop(false, do_shutdown_cb) }
  end
end
