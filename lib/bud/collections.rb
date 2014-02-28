module Bud
  ########
  #--
  # the collection types                                                                                                ``
  # each collection is partitioned into 4:
  # - pending holds tuples deferred til the next tick
  # - storage holds the "normal" tuples
  # - delta holds the delta for rhs's of rules during semi-naive
  # - new_delta will hold the lhs tuples currently being produced during s-n
  # - tick_delta holds \Union(delta_i) for each delta_i processed in fixpoint iteration i.
  #++

  class BudCollection
    include Enumerable

    attr_accessor :bud_instance  # :nodoc: all
    attr_reader :tabname, :cols, :key_cols # :nodoc: all
    attr_reader :struct
    attr_reader :new_delta, :pending # :nodoc: all
    attr_reader :wired_by, :scanner_cnt
    attr_accessor :invalidated, :rescan
    attr_accessor :is_source
    attr_accessor :accumulate_tick_deltas # updated in bud.do_wiring

    def initialize(name, bud_instance, given_schema=nil, defer_schema=false) # :nodoc: all
      @tabname = name
      @bud_instance = bud_instance
      @invalidated = true
      @is_source = true # unless it shows up on the lhs of some rule
      @scanner_cnt = 0
      @wired_by = []
      @accumulate_tick_deltas = false
      init_schema(given_schema) unless given_schema.nil? and defer_schema
      init_buffers
    end

    private
    def init_buffers
      init_storage
      init_pending
      init_deltas
    end

    public
    def init_schema(given_schema)
      given_schema ||= {[:key]=>[:val]}
      @given_schema = given_schema
      @cols, @key_cols = BudCollection.parse_schema(given_schema)

      # Check that no location specifiers appear in the schema. In the case of
      # channels, the location specifier has already been stripped from the
      # user-specified schema.
      @cols.each do |s|
        if s.to_s.start_with? "@"
          raise Bud::CompileError, "illegal use of location specifier (@) in column #{s} of non-channel collection #{tabname}"
        end
      end

      @key_colnums = @key_cols.map {|k| @cols.index(k)}
      @val_colnums = val_cols.map {|k| @cols.index(k)}

      if @cols.empty?
        @cols = nil
      else
        @struct = Bud::TupleStruct.new_struct(@cols)
        @structlen = @struct.members.length
      end
      setup_accessors
    end

    def qualified_tabname
      @qualified_tabname ||= @bud_instance.toplevel?  ? tabname : "#{@bud_instance.qualified_name}.#{tabname}".to_sym
    end

    def inspect
      "#{self.class}:#{self.object_id.to_s(16)} [#{qualified_tabname}]"
    end

    # The user-specified schema might come in two forms: a hash of Array =>
    # Array (key_cols => remaining columns), or simply an Array of columns (if
    # no key_cols were specified). Return a pair: [list of (all) columns, list
    # of key columns]
    private
    def self.parse_schema(given_schema)
      if given_schema.respond_to? :keys
        raise Bud::Error, "invalid schema for #{tabname}" if given_schema.length != 1
        key_cols = given_schema.keys.first
        val_cols = given_schema.values.first
      else
        key_cols = given_schema
        val_cols = []
      end

      cols = key_cols + val_cols
      cols.each do |c|
        if c.class != Symbol
          raise Bud::Error, "invalid column name \"#{c}\", type \"#{c.class}\""
        end
      end
      if cols.uniq.length < cols.length
        raise Bud::Error, "schema #{given_schema.inspect} contains duplicate names"
      end

      return [cols, key_cols]
    end

    # produces the schema in a format that is useful as the schema specification for another table
    public
    def schema
      return nil if @cols.nil?
      return key_cols if val_cols.empty?
      return { key_cols => val_cols }
    end

    # the columns of the collection's schema that are not part of the key
    public
    def val_cols # :nodoc: all
      @cols - @key_cols
    end

    # define methods to turn 'table.col' into a [table,col] pair
    # e.g. to support something like
    #    j = join link, path, {link.to => path.from}
    private
    def setup_accessors
      sc = @cols
      return if sc.nil?
      sc.each do |colname|
        if name_reserved? colname
          raise Bud::Error, "symbol :#{colname} reserved, cannot be used as column name for #{tabname}"
        end
      end

      # Setup schema accessors, which are class methods. Note that the same
      # table/column name might appear multiple times on the LHS of a single
      # join (e.g., (foo * bar).combos(foo.x => bar.y, foo.x => bar.z)). Because
      # the join predicates are represented as a hash, we need the two instances
      # of foo.x to be distinct values (otherwise the resulting hash will only
      # have a single key). Hence, we add a unique ID to the value returned by
      # schema accessors.
      @cols_access = Module.new do
        sc.each_with_index do |c, i|
          define_method c do
            @counter ||= 0
            @counter += 1
            [qualified_tabname, i, c, @counter]
          end
        end
      end
      self.extend @cols_access
    end

    private
    def name_reserved?(colname)
      reserved = eval "defined?(#{colname})"
      return false if reserved.nil?
      if reserved == "method" and (method(colname).arity == 0 or method(colname).arity == -1)
        begin
          ret = eval("#{colname}")
          if ret.kind_of? Array and ret.size == 3 and ret[0] == tabname
            return false # schema redefinition (see tupaccess above), so name is not considered reserved
          end
        rescue # in case calling method throws an error
        end
      end
      return true
    end

    # generate a tuple with the schema of this collection and nil values in each attribute
    public
    def null_tuple
      @struct.new
    end

    # project the collection to its key attributes
    public
    def keys
      self.pro{|t| get_key_vals(t)}
    end

    # project the collection to its non-key attributes
    public
    def values
      self.pro{|t| (self.key_cols.length..self.cols.length-1).map{|i| t[i]}}
    end

    # map each item in the collection into a string, suitable for placement in stdio
    public
    def inspected
      self.pro{|t| [t.inspect]}
    end

    # projection
    public
    def pro(the_name=tabname, the_schema=schema, &blk)
      if @bud_instance.wiring?
        pusher = to_push_elem(the_name, the_schema)
        # If there is no code block evaluate, use the scanner directly
        pusher = pusher.pro(&blk) unless blk.nil?
        pusher
      else
        rv = []
        self.each do |t|
          t = blk.call(t)
          rv << t unless t.nil?
        end
        rv
      end
    end

    # XXX: Although we support each_with_index over Bud collections, using it is
    # probably not a great idea: the index assigned to a given collection member
    # is not defined by the language semantics.
    def each_with_index(the_name=tabname, the_schema=schema, &blk)
      if @bud_instance.wiring?
        pusher = to_push_elem(the_name, the_schema)
        pusher.each_with_index(&blk)
      else
        super(&blk)
      end
    end

    # ruby 1.9 defines flat_map to return "a new array with the concatenated
    # results of running <em>block</em> once for every element". So we wire the
    # input to a pro(&blk), and wire the output of that pro to a group that does
    # accum.
    public
    def flat_map(&blk)
      if @bud_instance.wiring?
        pusher = self.pro(&blk)
        toplevel = @bud_instance.toplevel
        elem = Bud::PushElement.new(tabname, toplevel.this_rule_context, tabname)
        pusher.wire_to(elem)
        f = Proc.new do |t|
          t.each do |i|
            elem.push_out(i, false)
          end
          nil
        end
        elem.set_block(&f)
        toplevel.push_elems[[self.object_id, :flatten]] = elem
        elem
      else
        @storage.flat_map(&blk)
      end
    end

    public
    def sort(&blk)
      if @bud_instance.wiring?
        pusher = self.pro
        pusher.sort("sort#{object_id}".to_sym, @bud_instance, @cols, &blk)
      else
        @storage.values.sort(&blk)
      end
    end

    def rename(the_name, the_schema=nil, &blk)
      raise Bud::Error unless @bud_instance.wiring?
      # a scratch with this name should have been defined during rewriting
      unless @bud_instance.respond_to? the_name
        raise Bud::Error, "rename failed to define a scratch named #{the_name}"
      end
      pro(the_name, the_schema, &blk)
    end

    # By default, all tuples in any rhs are in storage or delta. Tuples in
    # new_delta will get transitioned to delta in the next iteration of the
    # evaluator (but within the current time tick).
    public
    def each(&block) # :nodoc: all
      each_from([@storage, @delta], &block)
    end

    public
    def each_raw(&block)
      each_from([@storage], &block)
    end

    public
    def each_delta(&block)
      each_from([@delta], &block)
    end

    public
    def each_tick_delta(&block)
      @tick_delta.each(&block)
    end

    public
    def invalidate_at_tick
      true # being conservative here as a default.
    end

    public
    def non_temporal_predecessors
      @wired_by.select {|e| e.outputs.include? self}
    end

    public
    def positive_predecessors
      @wired_by.select {|e| e.outputs.include?(self) || e.pendings.include?(self)}
    end

    public
    def tick_metrics
      strat_num = bud_instance.this_stratum
      addr = bud_instance.ip_port unless bud_instance.port.nil?
      key = { :addr=>addr, :tabname=>qualified_tabname,
              :strat_num=>strat_num}

      bud_instance.metrics[:collections] ||= {}
      bud_instance.metrics[:collections][key] ||= 0
      bud_instance.metrics[:collections][key] += 1
    end

    private
    def each_from(bufs, &block) # :nodoc: all
      do_metrics = bud_instance.options[:metrics]
      bufs.each do |b|
        b.each_value do |v|
          tick_metrics if do_metrics
          yield v
        end
      end
    end

    private
    def init_storage
      @storage = {}
    end

    private
    def init_pending
      @pending = {}
    end

    private
    def init_deltas
      @delta = {}
      @new_delta = {}
      @tick_delta = []
    end

    public
    def close # :nodoc: all
    end

    # checks for key +k+ in the key columns
    public
    def has_key?(k)
      check_enumerable(k)
      return false if k.nil? or self[k].nil?
      return true
    end

    # return item with key +k+
    public
    def [](k)
      # assumes that key is in storage or delta, but not both
      # is this enforced in do_insert?
      check_enumerable(k)
      t = @storage[k]
      return t.nil? ? @delta[k] : t
    end

    # checks for +item+ in the collection
    public
    def include?(item)
      return true if key_cols.nil?
      return false if item.nil?
      key = get_key_vals(item)
      return (item == self[key])
    end

    def length
      @storage.length + @delta.length
    end

    def empty?
      length == 0
    end

    # checks for an item for which +block+ produces a match
    public
    def exists?(&block)
      if length == 0
        return false
      elsif not block_given?
        return true
      else
        return ((detect{|t| yield t}).nil?) ? false : true
      end
    end

    private
    def raise_pk_error(new, old)
      key = get_key_vals(old)
      raise Bud::KeyConstraintError, "key conflict inserting #{new.inspect} into \"#{qualified_tabname}\": existing tuple #{old.inspect}, key = #{key.inspect}"
    end

    private
    def is_lattice_val(v)
      v.kind_of? Bud::Lattice
    end

    private
    def prep_tuple(o)
      return o if o.class == @struct
      if o.kind_of? Array
        if @struct.nil?
          sch = (1 .. o.length).map{|i| "c#{i}".to_sym}
          init_schema(sch)
        end
      elsif o.kind_of? Struct
        init_schema(o.members.map{|m| m.to_sym}) if @struct.nil?
      else
        raise Bud::TypeError, "array or struct type expected in \"#{qualified_tabname}\": #{o.inspect}"
      end

      @key_colnums.each do |i|
        next if i >= o.length
        if is_lattice_val(o[i])
          raise Bud::TypeError, "lattice value cannot be a key for #{qualified_tabname}: #{o[i].inspect}"
        end
      end
      if o.length > @structlen
        raise Bud::TypeError, "too many columns for \"#{qualified_tabname}\": #{o.inspect}"
      end

      return @struct.new(*o)
    end

    private
    def get_key_vals(t)
      t.values_at(*@key_colnums)
    end

    public
    def do_insert(t, store)
      if $BUD_DEBUG
        storetype = case store.object_id
                      when @storage.object_id; "storage"
                      when @pending.object_id; "pending"
                      when @delta.object_id; "delta"
                      when @new_delta.object_id; "new_delta"
                    end
        puts "#{qualified_tabname}.#{storetype} ==> #{t.inspect}"
      end
      return if t.nil? # silently ignore nils resulting from map predicates failing
      t = prep_tuple(t)
      key = get_key_vals(t)
      merge_to_buf(store, key, t, store[key])
    end

    # Merge "tup" with key values "key" into "buf". "old" is an existing tuple
    # with the same key columns as "tup" (if any such tuple exists). If "old"
    # exists and "tup" is not a duplicate, check whether the two tuples disagree
    # on a non-key, non-lattice value; if so, raise a PK error. Otherwise,
    # construct and return a merged tuple by using lattice merge functions.
    private
    def merge_to_buf(buf, key, tup, old)
      if old.nil?               # no matching tuple found
        buf[key] = tup
        return
      end
      return if tup == old      # ignore duplicates

      # Check for PK violation
      @val_colnums.each do |i|
        old_v = old[i]
        new_v = tup[i]

        unless old_v == new_v || (is_lattice_val(old_v) && is_lattice_val(new_v))
          raise_pk_error(tup, old)
        end
      end

      # Construct new tuple version. We discard the newly-constructed tuple if
      # merging every lattice field doesn't yield a new value.
      new_t = null_tuple
      saw_change = false
      @val_colnums.each do |i|
        if old[i] == tup[i]
          new_t[i] = old[i]
        else
          new_t[i] = old[i].merge(tup[i])
          saw_change = true if new_t[i].reveal != old[i].reveal
        end
      end

      if saw_change
        @key_colnums.each {|k| new_t[k] = old[k]}
        buf[key] = new_t
      end
    end

    public
    def insert(o, source=nil) # :nodoc: all
      # puts "insert: #{o} into #{qualified_tabname}"
      do_insert(o, @storage)
    end

    # instantaneously place an individual item from rhs into collection on lhs
    def <<(item)
      insert(item)
    end

    private
    def check_enumerable(o)
      unless o.nil? or o.class < Enumerable or o.class <= Proc
        raise Bud::TypeError, "collection #{qualified_tabname} expected Enumerable value, not #{o.inspect} (class = #{o.class})"
      end
    end

    # Assign self a schema, by hook or by crook.  If +o+ is schemaless *and*
    # empty, will leave @cols as is.
    private
    def establish_schema(o)
      # use o's schema if available
      deduce_schema(o)
      # else use arity of first non-nil tuple of o
      if @cols.nil?
        o.each do |t|
          next if t.nil?
          fit_schema(t.size)
          break
        end
      end
    end

    # Copy over the schema from +o+ if available
    private
    def deduce_schema(o)
      if @cols.nil? and o.class <= Bud::BudCollection and not o.cols.nil?
        # must have been initialized with defer_schema==true.  take schema from rhs
        init_schema(o.cols)
      end
      # if nothing available, leave @cols unchanged
    end

    # manufacture schema of the form [:c0, :c1, ...] with width = +arity+
    private
    def fit_schema(arity)
      # rhs is schemaless.  create schema from first tuple merged
      init_schema((0..arity-1).map{|indx| ("c"+indx.to_s).to_sym})
    end

    protected
    def add_merge_target
      toplevel = @bud_instance.toplevel
      if toplevel.done_bootstrap
        toplevel.merge_targets[toplevel.this_stratum] << self
      end
    end

    # This is used for two quite different purposes. If given a Bud collection
    # or dataflow element as an input, we assume we're being called to wire up
    # the push-based dataflow. If given an Enumerable consisting of Bud tuples,
    # we assume we're being called to insert the tuples (e.g., to support direct
    # insertion of tuples into Bud collections in a sync_do block).
    public
    def merge(o, buf=@delta) # :nodoc: all
      if o.class <= Bud::PushElement
        add_merge_target
        deduce_schema(o) if @cols.nil?
        o.wire_to self
      elsif o.class <= Bud::BudCollection
        add_merge_target
        deduce_schema(o) if @cols.nil?
        o.pro.wire_to self
      elsif o.class <= Proc
        add_merge_target
        tbl = register_coll_expr(o)
        tbl.pro.wire_to self
      elsif o.class <= Bud::LatticePushElement
        add_merge_target
        o.wire_to self
      elsif o.class <= Bud::LatticeWrapper
        add_merge_target
        o.to_push_elem.wire_to self
      else
        unless o.nil?
          o = o.uniq.compact if o.respond_to?(:uniq)
          check_enumerable(o)
          establish_schema(o) if @cols.nil?
          o.each {|i| do_insert(i, buf)}
        end
      end
    end

    def register_coll_expr(expr)
      coll_name = "expr_#{expr.object_id}"
      cols = (1..@cols.length).map{|i| "c#{i}".to_sym} unless @cols.nil?
      @bud_instance.coll_expr(coll_name.to_sym, expr, cols)
      @bud_instance.send(coll_name)
    end

    public
    # instantaneously merge items from collection +o+ into +buf+
    def <=(collection)
      unless bud_instance.toplevel.inside_tick
        raise Bud::CompileError, "illegal use of <= outside of bloom block, use <+ instead"
      end

      merge(collection)
    end

    # buffer items to be merged atomically at end of this timestep
    public
    def pending_merge(o) # :nodoc: all
      unless o.nil?
        o = o.uniq.compact if o.respond_to?(:uniq)
        check_enumerable(o)
        establish_schema(o) if @cols.nil?
        o.each{|i| self.do_insert(i, @pending)}
      end
    end

    public
    def flush ; end

    public
    superator "<+" do |o|
      if o.class <= Bud::PushElement
        add_merge_target
        o.wire_to(self, :pending)
      elsif o.class <= Bud::BudCollection
        add_merge_target
        o.pro.wire_to(self, :pending)
      elsif o.class <= Proc
        add_merge_target
        tbl = register_coll_expr(o)
        tbl.pro.wire_to(self, :pending)
      elsif o.class <= Bud::LatticePushElement
        add_merge_target
        o.wire_to(self, :pending)
      elsif o.class <= Bud::LatticeWrapper
        add_merge_target
        o.to_push_elem.wire_to(self, :pending)
      else
        pending_merge(o)
      end
    end

    superator "<~" do |o|
      # Overridden when <~ is defined (i.e., channels and terminals)
      raise Bud::CompileError, "#{tabname} cannot appear on the lhs of a <~ operator"
    end

    def tick
      raise Bud::Error, "tick must be overriden in #{self.class}"
    end

    # move deltas to storage, and new_deltas to deltas.
    # return true if new deltas were found
    public
    def tick_deltas # :nodoc: all
      unless @delta.empty?
        puts "#{qualified_tabname}.tick_deltas delta --> storage (#{@delta.size} elems)" if $BUD_DEBUG
        @storage.merge!(@delta)
        @tick_delta.concat(@delta.values) if accumulate_tick_deltas
        @delta.clear
      end

      unless @new_delta.empty?
        puts "#{qualified_tabname}.tick_deltas new_delta --> delta (#{@new_delta.size} elems)" if $BUD_DEBUG

        # NB: key conflicts between two new_delta tuples are detected in
        # do_insert().
        @new_delta.each_pair do |key, tup|
          merge_to_buf(@delta, key, tup, @storage[key])
        end
        @new_delta.clear
        return !(@delta.empty?)
      end
      return false # delta empty; another fixpoint iter not required.
    end

    public
    def add_rescan_invalidate(rescan, invalidate)
      # No change. Most collections don't need to rescan on every tick (only do
      # so on negate). Also, there's no cache to invalidate by default.
      # Scratches and PushElements override this method.
    end

    def bootstrap
      unless @pending.empty?
        @delta = @pending
        @pending = {}
      end
    end

    public
    def flush_deltas
      if $BUD_DEBUG
        puts "#{qualified_tabname}.flush delta --> storage" unless @delta.empty?
        puts "#{qualified_tabname}.flush new_delta --> storage" unless @new_delta.empty?
      end
      unless @delta.empty?
        @storage.merge!(@delta)
        @tick_delta.concat(@delta.values) if accumulate_tick_deltas
        @delta.clear
      end
      unless @new_delta.empty?
        @storage.merge!(@new_delta)
        @new_delta.clear
      end
      # @tick_delta kept around for higher strata.
    end

    public
    def to_push_elem(the_name=tabname, the_schema=schema)
      # if no push source yet, set one up
      toplevel = @bud_instance.toplevel
      this_stratum = toplevel.this_stratum
      oid = self.object_id
      unless toplevel.scanners[this_stratum][[oid, the_name]]
        scanner = Bud::ScannerElement.new(the_name, @bud_instance,
                                          self, the_schema)
        toplevel.scanners[this_stratum][[oid, the_name]] = scanner
        toplevel.push_sources[this_stratum][[oid, the_name]] = scanner
        @scanner_cnt += 1
      end
      return toplevel.scanners[this_stratum][[oid, the_name]]
    end

    ######## aggs

    private
    # we only do grouping during first iteration of stratum.  group and argagg should
    # never deal with deltas.  This assumes that stratification is done right, and it will
    # be sensitive to bugs in the stratification!
    def agg_in
      if not respond_to?(:bud_instance) or bud_instance.nil?
        return self
      else
        return []
      end
    end

    # a generalization of argmin/argmax to arbitrary exemplary aggregates.
    # for each distinct value of the grouping key columns, return the items in that group
    # that have the value of the exemplary aggregate +aggname+
    public
    def argagg(aggname, gbkey_cols, collection, &blk)
      elem = to_push_elem
      gbkey_cols = gbkey_cols.map{|k| canonicalize_col(k)} unless gbkey_cols.nil?
      retval = elem.argagg(aggname, gbkey_cols, canonicalize_col(collection), &blk)
      # PushElement inherits the schema accessors from this Collection
      retval.extend @cols_access
      retval
    end

    # for each distinct value of the grouping key columns, return the items in
    # that group that have the minimum value of the attribute +col+. Note that
    # multiple tuples might be returned.
    public
    def argmin(gbkey_cols, col, &blk)
      argagg(:min, gbkey_cols, col, &blk)
    end

    # for each distinct value of the grouping key columns, return the items in
    # that group that have the maximum value of the attribute +col+. Note that
    # multiple tuples might be returned.
    public
    def argmax(gbkey_cols, col, &blk)
      argagg(:max, gbkey_cols, col, &blk)
    end

    # form a collection containing all pairs of items in +self+ and items in
    # +collection+
    public
    def *(collection)
      return to_push_elem.join(collection)
    end

    def prep_aggpairs(aggpairs)
      aggpairs.map do |ap|
        agg, *rest = ap
        if rest.empty?
          [agg]
        else
          [agg] + rest.map {|c| canonicalize_col(c)}
        end
      end
    end

    def group(key_cols, *aggpairs, &blk)
      key_cols = key_cols.map{|k| canonicalize_col(k)} unless key_cols.nil?
      aggpairs = prep_aggpairs(aggpairs)
      return to_push_elem.group(key_cols, *aggpairs, &blk)
    end

    def notin(collection, *preds, &blk)
      elem1 = to_push_elem
      elem2 = collection.to_push_elem
      return elem1.notin(elem2, *preds, &blk)
    end

    def canonicalize_col(col)
      col.class <= Symbol ? self.send(col) : col
    end

    def reduce(initial, &blk)
      return to_push_elem.reduce(initial, &blk)
    end

    public
    def uniquify_tabname # :nodoc: all
      # just append current number of microseconds
      @tabname = (@tabname.to_s + Time.new.tv_usec.to_s).to_sym
    end
  end

  class BudScratch < BudCollection # :nodoc: all
    # We don't need to accumulate @tick_delta separately from @storage for
    # scratch collections, since @storage for scratches doesn't persistent
    # across ticks (semantics-wise, at least).
    def accumulate_tick_deltas
      false
    end

    # tick_delta for scratches is @storage, so iterate over that instead
    public
    def each_tick_delta(&block)
      @storage.each_value(&block)
    end

    public
    def tick  # :nodoc: all
      @delta.clear
      if not @pending.empty?
        invalidate_cache
        @delta = @pending
        @pending = {}
      elsif is_source
        invalidate_cache
      end
      raise Bud::Error, "orphaned tuples in @new_delta for #{qualified_tabname}" unless @new_delta.empty?
    end

    public
    def invalidate_at_tick
      is_source      # rescan always only if this scratch is a source.
    end

    public
    def add_rescan_invalidate(rescan, invalidate)
      srcs = non_temporal_predecessors

      # XXX: this seems wrong. We might rescan a node for many reasons (e.g.,
      # because another one of the node's outputs needs to be refilled). We only
      # need to invalidate + rescan this scratch if one of the inputs to this
      # collection is *invalidated*.
      if srcs.any? {|e| rescan.member? e}
        invalidate << self
        rescan.merge(srcs)
      end
    end

    public
    def invalidate_cache
      puts "#{qualified_tabname} invalidated" if $BUD_DEBUG
      # for scratches, storage is a cached value
      @invalidated = true
      @storage.clear
    end
  end

  class BudInputInterface < BudScratch
  end

  class BudOutputInterface < BudScratch
  end

  class BudTemp < BudScratch # :nodoc: all
  end

  # Channels are a different type of collection in that they represent two
  # distinct collections, one each for incoming and outgoing.  The incoming side
  # makes use of @storage and @delta, whereas the outgoing side only deals with
  # @pending. XXX Maybe we should be using aliases instead.
  class BudChannel < BudCollection
    attr_reader :locspec_idx # :nodoc: all

    def initialize(name, bud_instance, given_schema=nil, loopback=false) # :nodoc: all
      given_schema ||= [:@address, :val]
      @is_loopback = loopback
      @locspec_idx = nil
      @wire_buf = StringIO.new
      @packer = MessagePack::Packer.new(@wire_buf)

      # We're going to mutate the caller's given_schema (to remove the location
      # specifier), so make a deep copy first. We also save a ref to the
      # unmodified given_schema.
      @raw_schema = given_schema
      given_schema = Marshal.load(Marshal.dump(given_schema))

      unless @is_loopback
        the_cols, the_key_cols = BudCollection.parse_schema(given_schema)
        spec_count = the_cols.count {|c| c.to_s.start_with? "@"}
        if spec_count == 0
          raise Bud::Error, "missing location specifier for channel '#{name}'"
        end
        if spec_count > 1
          raise Bud::Error, "multiple location specifiers for channel '#{name}'"
        end

        the_val_cols = the_cols - the_key_cols
        @locspec_idx = remove_at_sign!(the_key_cols)
        if @locspec_idx.nil?
          val_idx = remove_at_sign!(the_val_cols)
          @locspec_idx = val_idx + the_key_cols.length
        end

        # We mutate the hash key above, so we need to recreate the hash
        # XXX: ugh, hacky
        if given_schema.respond_to? :keys
          given_schema = {the_key_cols => the_val_cols}
        end
      end

      super(name, bud_instance, given_schema)
    end

    def bootstrap
      # override BudCollection; pending should not be moved into delta.
    end

    private
    def remove_at_sign!(cols)
      i = cols.find_index {|c| c.to_s.start_with? "@"}
      unless i.nil?
        cols[i] = cols[i].to_s.delete('@').to_sym
      end
      return i
    end

    private
    def split_locspec(t, idx)
      begin
        lsplit = t[idx].split(':')
        lsplit[1] = lsplit[1].to_i
        return lsplit
      rescue Exception => e
        raise Bud::Error, "illegal location specifier in tuple #{t.inspect} for channel \"#{qualified_tabname}\": #{e.to_s}"
      end
    end

    public
    def tick # :nodoc: all
      @storage.clear
      @invalidated = true
      # Note that we do not clear @pending here: if the user inserted into the
      # channel manually (e.g., via <~ from inside a sync_do block), we send the
      # message at the end of the current tick.
    end

    public
    def invalidate_cache
    end

    public
    def flush # :nodoc: all
      toplevel = @bud_instance.toplevel
      @pending.each_value do |t|
        if @is_loopback
          ip = toplevel.ip
          port = toplevel.port
          the_locspec = [ip, port]
        else
          the_locspec = split_locspec(t, @locspec_idx)
          raise Bud::Error, "'#{t[@locspec_idx]}', channel '#{@tabname}'" if the_locspec[0].nil? or the_locspec[1].nil? or the_locspec[0] == '' or the_locspec[1] == ''
        end
        puts "channel #{qualified_tabname}.send: #{t}" if $BUD_DEBUG

        # Convert the tuple into a suitable wire format. Because MsgPack cannot
        # marshal arbitrary Ruby objects that we need to send via channels (in
        # particular, lattice values and Class instances), we first encode such
        # values using Marshal, and then encode the entire tuple with
        # MsgPack. Obviously, this is gross. The wire format also includes an
        # array of indices, indicating which fields hold Marshall'd values.
        @packer.write_array_header(3)
        @packer.write(qualified_tabname.to_s)
        # The second element, wire_tuple, is an array.  We will write it one
        # element at a time:
        @packer.write_array_header(t.length)
        @packer.flush
        marshall_indexes = []
        t.each_with_index do |f,i|
          # Performance optimization for cases where we know that we can't
          # marshal the field using MsgPack:
          if [Bud::Lattice, Class].any?{|t| f.class <= t}
            marshall_indexes << i
            @wire_buf << Marshal.dump(f).to_msgpack
          else
            begin
              @wire_buf << f.to_msgpack
            rescue NoMethodError
              # If MsgPack can't marshal the field, fall back to Marshal.
              # This handles fields that contain nested non-MsgPack-able
              # objects (in these cases, the entire field is Marshal'd.)
              marshall_indexes << i
              @wire_buf << Marshal.dump(f).to_msgpack
            end
          end
        end
        @packer.write(marshall_indexes)
        @packer.flush
        toplevel.dsock.send_datagram(@wire_buf.string,
                                     the_locspec[0], the_locspec[1])

        # Reset output buffer
        @wire_buf.rewind
        @wire_buf.truncate(0)
      end
      @pending.clear
    end

    public
    # project to the non-address fields
    def payloads(&blk)
      return self.pro(&blk) if @is_loopback

      if @payload_struct.nil?
        payload_cols = cols.dup
        payload_cols.delete_at(@locspec_idx)
        @payload_struct = Bud::TupleStruct.new(*payload_cols)
        @payload_colnums = payload_cols.map {|k| cols.index(k)}
      end

      retval = self.pro do |t|
        @payload_struct.new(*t.values_at(*@payload_colnums))
      end
      retval = retval.pro(&blk) unless blk.nil?
      return retval
    end

    superator "<~" do |o|
      if o.class <= Bud::PushElement
        o.wire_to(self, :pending)
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to(self, :pending)
      elsif o.class <= Proc
        tbl = register_coll_expr(o)
        tbl.pro.wire_to(self, :pending)
      elsif o.class <= Bud::LatticePushElement
        add_merge_target
        o.wire_to(self, :pending)
      elsif o.class <= Bud::LatticeWrapper
        add_merge_target
        o.to_push_elem.wire_to(self, :pending)
      else
        pending_merge(o)
      end
    end

    superator "<+" do |o|
      raise Bud::CompileError, "illegal use of <+ with channel '#{@tabname}' on left"
    end

    undef merge

    def <=(o)
      raise Bud::CompileError, "illegal use of <= with channel '#{@tabname}' on left"
    end
  end

  class BudTerminal < BudScratch # :nodoc: all
    def initialize(name, bud_instance, prompt=false) # :nodoc: all
      super(name, bud_instance, [:line])
      @prompt = prompt
    end

    public
    def start_stdin_reader # :nodoc: all
      Thread.new do
        begin
          while true
            break unless read_line
          end
        rescue Exception
          puts "terminal reader thread failed: #{$!}"
          print $!.backtrace.join("\n")
          exit
        end
      end
    end

    # XXX: Ugly hack. Rather than sending terminal data to EM via UDP, we should
    # add the terminal file descriptor to the EM event loop.
    private
    def read_line
      get_out_io.print("#{tabname} > ") if @prompt

      toplevel = @bud_instance.toplevel
      in_io = toplevel.options[:stdin]
      input_str = in_io.gets
      return false if input_str.nil? # Hit EOF
      input_str.chomp!

      EventMachine::schedule do
        socket = EventMachine::open_datagram_socket("127.0.0.1", 0)
        socket.send_datagram([tabname, [input_str], []].to_msgpack,
                             toplevel.ip, toplevel.port)
      end

      return true
    end

    public
    def bootstrap
      # override BudCollection; pending should not be moved into delta.
    end

    public
    def flush #:nodoc: all
      out_io = get_out_io
      @pending.each_value {|p| out_io.puts p[0]}
      out_io.flush
      @pending.clear
    end

    public
    def invalidate_at_tick
      true
    end

    public
    def tick #:nodoc: all
      unless @pending.empty?
        @delta = @pending       # pending used for input tuples in this case
        @tick_delta = @pending.values
        @pending.clear
      else
        @storage.clear
        @delta.clear
        @tick_delta.clear
      end
      @invalidated = true       # channels and terminals are always invalidated
    end

    public
    def invalidate_cache
    end

    undef merge

    public
    def <=(o) #:nodoc: all
      raise Bud::CompileError, "illegal use of <= with terminal '#{@tabname}' on left"
    end

    superator "<+" do |o|
      raise Bud::CompileError, "illegal use of <+ with terminal '#{@tabname}' on left"
    end

    superator "<~" do |o|
      if o.class <= Bud::PushElement
        o.wire_to(self, :pending)
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to(self, :pending)
      elsif o.class <= Proc
        tbl = register_coll_expr(o)
        tbl.pro.wire_to(self, :pending)
      else
        pending_merge(o)
      end
    end

    private
    def get_out_io
      rv = @bud_instance.toplevel.options[:stdout]
      rv ||= $stdout
      if rv.closed?
        raise Bud::Error, "attempt to write to closed terminal '#{tabname}'"
      end
      rv
    end
  end

  class BudPeriodic < BudScratch # :nodoc: all
    def <=(o)
      raise Bud::CompileError, "illegal use of <= with periodic '#{tabname}' on left"
    end

    superator "<~" do |o|
      raise Bud::CompileError, "illegal use of <~ with periodic '#{tabname}' on left"
    end

    superator "<-" do |o|
      raise Bud::CompileError, "illegal use of <- with periodic '#{tabname}' on left"
    end

    superator "<+" do |o|
      raise Bud::CompileError, "illegal use of <+ with periodic '#{tabname}' on left"
    end

    def tick
      @tick_delta.clear
      @delta.clear
      @invalidated = true
      unless pending.empty?
        @delta = @pending
        @pending = {}
      end
    end
  end

  class BudPersistentCollection < BudCollection
    public
    def invalidate_at_tick
      false # rescan required only when negated.
    end

    public
    def invalidate_cache
      raise Bud::Error, "abstract method not implemented by derived class #{self.class}"
    end
  end

  class BudTable < BudPersistentCollection # :nodoc: all
    def initialize(name, bud_instance, given_schema) # :nodoc: all
      super(name, bud_instance, given_schema)
      @to_delete = []
      @to_delete_by_key = []
    end

    public
    def tick #:nodoc: all
      if $BUD_DEBUG
        puts "#{tabname}.storage -= pending deletes" unless @to_delete.empty? and @to_delete_by_key.empty?
        puts "#{tabname}.delta += pending" unless @pending.empty?
      end
      @tick_delta.clear
      deleted = nil
      @to_delete.each do |tuple|
        keycols = get_key_vals(tuple)
        if @storage[keycols] == tuple
          v = @storage.delete keycols
          deleted ||= v
        end
      end
      @to_delete_by_key.each do |tuple|
        v = @storage.delete(get_key_vals(tuple))
        deleted ||= v
      end

      @invalidated = (not deleted.nil?)
      puts "table #{qualified_tabname} invalidated" if $BUD_DEBUG and @invalidated

      @pending.each do |key, tup|
        merge_to_buf(@delta, key, tup, @storage[key])
      end
      @to_delete = []
      @to_delete_by_key = []
      @pending = {}
    end

    def invalidated=(val)
      # Might be reset to false at end-of-tick, but shouldn't be set to true
      raise Bud::Error, "cannot set invalidate on table '#{@tabname}'" if val
      super
    end

    def pending_delete(o)
      if o.class <= Bud::PushElement
        add_merge_target
        o.wire_to(self, :delete)
      elsif o.class <= Bud::BudCollection
        add_merge_target
        o.pro.wire_to(self, :delete)
      elsif o.class <= Proc
        add_merge_target
        tbl = register_coll_expr(o)
        tbl.pro.wire_to(self, :delete)
      elsif o.class <= Bud::LatticePushElement
        add_merge_target
        o.wire_to(self, :delete)
      elsif o.class <= Bud::LatticeWrapper
        add_merge_target
        o.to_push_elem.wire_to(self, :delete)
      else
        unless o.nil?
          o = o.uniq.compact if o.respond_to?(:uniq)
          check_enumerable(o)
          establish_schema(o) if @cols.nil?
          o.each{|i| @to_delete << prep_tuple(i)}
        end
      end
    end
    superator "<-" do |o|
      pending_delete(o)
    end

    public
    def pending_delete_keys(o)
      if o.class <= Bud::PushElement
        o.wire_to(self, :delete_by_key)
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to(self, :delete_by_key)
      elsif o.class <= Proc
        tbl = register_coll_expr(o)
        tbl.pro.wire_to(self, :delete_by_key)
      else
        unless o.nil?
          o = o.uniq.compact if o.respond_to?(:uniq)
          check_enumerable(o)
          establish_schema(o) if @cols.nil?
          o.each{|i| @to_delete_by_key << prep_tuple(i)}
        end
      end
    end

    public
    def invalidate_cache
      # No cache to invalidate. Also, tables do not invalidate dependents,
      # because their own state is not considered invalidated; that happens only
      # if there were pending deletes at the beginning of a tick (see tick())
      puts "******** invalidate_cache called on table '#{@tabname}'" if $BUD_DEBUG
    end

    public
    superator "<+-" do |o|
      pending_delete_keys(o)
      self <+ o
    end
    public
    superator "<-+" do |o|
      self <+- o
    end
  end

  class BudReadOnly < BudCollection # :nodoc: all
    superator "<+" do |o|
      raise Bud::CompileError, "illegal use of <+ with read-only collection '#{@tabname}' on left"
    end
    public
    def merge(o)  #:nodoc: all
      raise Bud::CompileError, "illegal use of <= with read-only collection '#{@tabname}' on left"
    end
    public
    def invalidate_cache
    end

    public
    def invalidate_at_tick
      true
    end
  end

  class BudCollExpr < BudReadOnly # :nodoc: all
    def initialize(name, bud_instance, expr, given_schema=nil, defer_schema=false)
      super(name, bud_instance, given_schema, defer_schema)
      @expr = expr
      @invalidated = true
    end

    def tick
      @invalidated = true
    end

    public
    def each(&block)
      v = @expr.call
      return if v.nil? or v == [nil]

      # XXX: Gross hack. We want to support RHS expressions that do not
      # necessarily return BudCollections (they might instead return lattice
      # values or hashes). Since it isn't easy to distinguish between these two
      # cases statically, instead we just always use CollExpr; at runtime, if
      # the value doesn't look like a traditional Bloom collection, we don't try
      # to break it up into tuples.
      if v.class <= Array || v.class <= BudCollection
        v.each(&block)
      else
        yield v
      end
    end

    public
    def each_raw(&block)
      each(&block)
    end
  end

  class BudFileReader < BudReadOnly # :nodoc: all
    def initialize(name, filename, bud_instance) # :nodoc: all
      super(name, bud_instance, {[:lineno] => [:text]})
      @filename = filename
      @storage = {}
      # NEEDS A TRY/RESCUE BLOCK
      @fd = File.open(@filename, "r")
      @linenum = 0
      @invalidated = true
    end
    
    def tick
      @invalidated = true
    end

    public
    def each_raw(&block) # :nodoc: all
      while (l = @fd.gets)
        t = [@linenum, l.strip]
        @linenum += 1
        tick_metrics if bud_instance.options[:metrics]
        yield t
      end
    end

    public
    def each(&blk)
      each_raw(&blk)
    end
  end
end

module Enumerable
  public
  # We rewrite "map" calls in Bloom blocks to invoke the "pro" method
  # instead. This is fine when applied to a BudCollection; when applied to a
  # normal Enumerable, just treat pro as an alias for map.
  def pro(&blk)
    map(&blk)
  end
end
