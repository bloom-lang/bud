require 'msgpack'

module Bud
  ########
  #--
  # the collection types
  # each collection is partitioned into 4:
  # - pending holds tuples deferred til the next tick
  # - storage holds the "normal" tuples
  # - delta holds the delta for rhs's of rules during semi-naive
  # - new_delta will hold the lhs tuples currently being produced during s-n
  #++

  class BudCollection
    include Enumerable

    attr_accessor :bud_instance, :locspec_idx, :tabname  # :nodoc: all
    attr_reader :schema # :nodoc: all
    attr_reader :storage, :delta, :new_delta, :pending # :nodoc: all

    def initialize(name, bud_instance, given_schema=nil, defer_schema=false) # :nodoc: all
      @tabname = name
      @bud_instance = bud_instance
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
      @schema, @key_cols = BudCollection.parse_schema(given_schema)
      @key_colnums = key_cols.map {|k| schema.index(k)}
      setup_accessors
    end

    # The user-specified schema might come in two forms: a hash of Array =>
    # Array (key_cols => remaining columns), or simply an Array of columns (if no
    # key_cols were specified). Return a pair: [list of columns in entire tuple,
    # list of key columns]
    private
    def self.parse_schema(given_schema)
      if given_schema.respond_to? :keys
        raise BudError, "invalid schema for #{tabname}" if given_schema.length != 1
        key_cols = given_schema.keys.first
        val_cols = given_schema.values.first
      else
        key_cols = given_schema
        val_cols = []
      end

      schema = key_cols + val_cols
      schema.each do |s|
        if s.class != Symbol
          raise BudError, "Invalid schema element \"#{s}\", type \"#{s.class}\""
        end
      end
      if schema.uniq.length < schema.length
        raise BudError, "schema #{given_schema} contains duplicate names"
      end

      return [schema, key_cols]
    end

    public
    def clone_empty #:nodoc: all
      self.class.new(tabname, bud_instance, @given_schema)
    end

    # subset of the schema (i.e. an array of attribute names) that forms the key
    public
    def key_cols
      @key_cols
    end

    # subset of the schema (i.e. an array of attribute names) that is not in the key
    public
    def val_cols # :nodoc: all
      schema - key_cols
    end

    # define methods to turn 'table.col' into a [table,col] pair
    # e.g. to support something like
    #    j = join link, path, {link.to => path.from}
    private
    def setup_accessors
      sc = @schema
      sc.each do |colname|
        reserved = eval "defined?(#{colname})"
        unless (reserved.nil? or
          (reserved == "method" and method(colname).arity == -1 and (eval("#{colname}"))[0] == self.tabname))
          raise BudError, "symbol :#{colname} reserved, cannot be used as column name for #{tabname}"
        end
      end

      # set up schema accessors, which are class methods
      @schema_access = Module.new do
        sc.each_with_index do |c, i|
          define_method c do
            [@tabname, i, c]
          end
        end
      end
      self.extend @schema_access

      # now set up a Module for tuple accessors, which are instance methods
      @tupaccess = Module.new do
        sc.each_with_index do |colname, offset|
          define_method colname do
            self[offset]
          end
        end
      end
    end

    # define methods to access tuple attributes by column name
    public
    def tuple_accessors(tup)
      tup.extend @tupaccess
    end

    # generate a tuple with the schema of this collection and nil values in each attribute
    public
    def null_tuple
      Array.new(@schema.length)
    end

    # project the collection to its key attributes
    public
    def keys
      self.pro{|t| @key_colnums.map {|i| t[i]}}
    end

    # project the collection to its non-key attributes
    public
    def values
      self.pro{|t| (self.key_cols.length..self.schema.length-1).map{|i| t[i]}}
    end

    # map each item in the collection into a string, suitable for placement in stdio
    public
    def inspected
      self.pro{|t| [t.inspect]}
    end

    # projection
    public
    def pro(the_name = tabname, the_schema = schema, &blk)
      # if @bud_instance.stratum_first_iter
      # puts "adding pusher.pro to #{tabname}"
      pusher, delta_pusher = to_push_elem(the_name, the_schema)
      pusher_pro = pusher.pro(&blk)
      pusher_pro.elem_name = the_name
      pusher_pro.tabname = the_name
      pusher_pro.schema = the_schema
      delta_pusher.wire_to(pusher_pro)
      pusher_pro
    end
    
    # ruby 1.9 defines flat_map to return "a new array with the concatenated results of running
    # <em>block</em> once for every element". So we wire the input to a pro(&blk), and wire the output
    # of that pro to a group that does accum.
    public
    def flat_map(&blk)
      pusher = self.pro(&blk)
      elem = Bud::PushElement.new(tabname, @bud_instance)
      pusher.wire_to(elem)
      f = Proc.new do |t| 
        t.each do |i| 
          elem.push_out(i,false)
        end
        nil
      end
      elem.set_block(&f)
      @bud_instance.push_elems[[self.object_id,:flatten]] = elem
      return elem
    end
    
    public 
    def sort(&blk)
      pusher = self.pro
      pusher.sort(@name, @bud_instance, @schema, &blk)
    end
    
    def rename(the_name, the_schema=nil)
      # a scratch with this name should have been defined during rewriting
      raise(BudError, "rename failed to define a scratch named #{the_name}") unless @bud_instance.respond_to? the_name
      retval = pro(the_name, the_schema)
      retval.init_schema(the_schema)
      retval
    end
    
    # def to_enum
    #   pusher = self.pro
    #   pusher.to_enum
    # end
    
    # By default, all tuples in any rhs are in storage or delta. Tuples in
    # new_delta will get transitioned to delta in the next iteration of the
    # evaluator (but within the current time tick).
    public
    def each(&block) # :nodoc: all
      each_from([@storage, @delta], &block)
    end
    
    public
    def each_raw(&block)
      @storage.each_value(&block)
    end

    public
    def tick_metrics
      strat_num = bud_instance.this_stratum
      rule_num = bud_instance.this_rule
      addr = nil
      addr = bud_instance.ip_port unless bud_instance.port.nil?
      rule_txt = nil
      bud_instance.metrics[:collections] ||= {}
      bud_instance.metrics[:collections][{:addr=>addr, :tabname=>tabname, :strat_num=>strat_num, :rule_num=>rule_num}] ||= 0
      bud_instance.metrics[:collections][{:addr=>addr, :tabname=>tabname, :strat_num=>strat_num, :rule_num=>rule_num}] += 1
    end
    
    private
    def each_from(bufs, &block) # :nodoc: all
      bufs.each do |b|
        b.each_value do |v|
          tick_metrics if bud_instance and bud_instance.options[:metrics]
          yield tuple_accessors(v)
        end
      end
    end

    public
    def each_from_sym(buf_syms, &block) # :nodoc: all
      bufs = buf_syms.map do |s|
        case s
        when :storage then @storage
        when :delta then @delta
        when :new_delta then @new_delta
        else raise BudError, "bad symbol passed into each_from_sym"
        end
      end
      each_from(bufs, &block)
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
      t = @storage[k]
      return t.nil? ? @delta[k] : tuple_accessors(t)
    end

    # checks for +item+ in the collection
    public
    def include?(item)
      return true if key_cols.nil? or (key_cols.empty? and length > 0)
      return false if item.nil? or item.empty?
      key = @key_colnums.map{|i| item[i]}
      return (item == self[key])
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
    def raise_pk_error(new_guy, old)
      keycols = @key_colnums.map{|i| old[i]}
      raise KeyConstraintError, "Key conflict inserting #{new_guy.inspect} into \"#{tabname}\": existing tuple #{old.inspect}, key_cols = #{keycols.inspect}"
    end

    private
    def prep_tuple(o)
      unless o.respond_to?(:length) and o.respond_to?(:[])
        raise BudTypeError, "non-indexable type inserted into \"#{tabname}\": #{o.inspect}"
      end
      if o.class <= String
        raise BudTypeError, "String value used as a fact inserted into \"#{tabname}\": #{o.inspect}"
      end

      fit_schema(o.length) if schema.nil?
      if o.length < schema.length then
        # if this tuple has too few fields, pad with nil's
        old = o.clone
        (o.length..schema.length-1).each{|i| o << nil}
        # puts "in #{@tabname}, converted #{old.inspect} to #{o.inspect}"
      elsif o.length > schema.length then
        # if this tuple has more fields than usual, bundle up the
        # extras into an array
        o = (0..(schema.length - 1)).map{|c| o[c]} << (schema.length..(o.length - 1)).map{|c| o[c]}
      end
      return o
    end

    public
    def do_insert(o, store)
      return if o.nil? # silently ignore nils resulting from map predicates failing
      o = prep_tuple(o)
      keycols = @key_colnums.map{|i| o[i]}

      old = store[keycols]
      if old.nil?
        store[keycols] = o
      else
        raise_pk_error(o, old) unless old == o
      end
    end

    public
    def insert(o, source=nil) # :nodoc: all
      # puts "insert: #{o.inspect} into #{tabname}"
      do_insert(o, @storage)
    end

    # instantaneously place an individual item from rhs into collection on lhs
    def <<(item)
      insert(item)
    end

    private
    def check_enumerable(o)
      unless o.nil? or o.class < Enumerable
        raise BudTypeError, "Collection #{tabname} expected Enumerable value, not #{o.inspect} (class = #{o.class})"
      end
    end

    # Assign self a schema, by hook or by crook.  If +o+ is schemaless *and*
    # empty, will leave @schema as is.
    private
    def establish_schema(o)
      # use o's schema if available
      deduce_schema(o)
      # else use arity of first non-nil tuple of o
      if @schema.nil?
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
      if @schema.nil? and o.class <= Bud::BudCollection and not o.schema.nil?
        # must have been initialized with defer_schema==true.  take schema from rhs
        init_schema(o.schema)
      end
      # if nothing available, leave @schema unchanged
    end

    # manufacture schema of the form [:c0, :c1, ...] with width = +arity+
    private
    def fit_schema(arity)
      # rhs is schemaless.  create schema from first tuple merged
      init_schema((0..arity-1).map{|indx| ("c"+indx.to_s).to_sym})
    end

    private
    def include_any_buf?(t, key_vals)
      bufs = [self, @delta, @new_delta]
      bufs.each do |b|
        old = b[key_vals]
        next if old.nil?
        if old != t
          raise_pk_error(t, old)
        else
          return true
        end
      end
      return false
    end

    public
    def merge(o, buf=@delta) # :nodoc: all
      @bud_instance.merge_targets[@bud_instance.this_stratum][self] = true if o.class <= Bud::BudCollection
      if o.class <= Bud::PushElement
        o.wire_to self
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to self
      else
        unless o.nil?
          o = o.uniq.compact if o.respond_to?(:uniq)
          check_enumerable(o)
          establish_schema(o) if @schema.nil?
          o.each {|i| do_insert(i, buf)}
        end
      end
      return self
    end

    public
    # instantaneously merge items from collection +o+ into +buf+
    def <=(collection)
      merge(collection)
    end

    # buffer items to be merged atomically at end of this timestep
    public
    def pending_merge(o) # :nodoc: all
      if o.class <= Bud::PushElement
        o.wire_to_pending self
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to_pending self
      else
        unless o.nil?
           o = o.uniq.compact if o.respond_to?(:uniq)
           check_enumerable(o)
           establish_schema(o) if @schema.nil?
           o.each {|i| do_insert(i, @pending)}
         end
      end
      return self
    end

    public
    def flush ; end

    public
    superator "<+" do |o|
      pending_merge o
    end
    
    # Called at the end of each timestep: prepare the collection for the next
    # timestep.
    public
    def tick  # :nodoc: all
      @storage = @pending
      @pending = {}
      raise BudError, "orphaned tuples in @delta for #{@tabname}" unless @delta.empty?
      raise BudError, "orphaned tuples in @new_delta for #{@tabname}" unless @new_delta.empty?
    end

    # move deltas to storage, and new_deltas to deltas.
    # return true if new deltas were found
    public
    def tick_deltas # :nodoc: all
      # assertion: intersect(@storage, @delta) == nil
      @storage.merge!(@delta)
      @delta = @new_delta
      @new_delta = {}
      return !(@delta == {})
    end
    
    public
    def flush_deltas
      @storage.merge!(@delta)
      @storage.merge!(@new_delta)
      @delta = {}
      @new_delta = {}
    end
    
    public
    def to_push_elem(the_name=tabname, the_schema=schema)
      # if no push source yet, set one up
      unless @bud_instance.scanners[@bud_instance.this_stratum][the_name]
        @bud_instance.scanners[@bud_instance.this_stratum][the_name] = Bud::ScannerElement.new(the_name, @bud_instance, self, the_schema)
        @bud_instance.push_sources[@bud_instance.this_stratum][the_name] = @bud_instance.scanners[@bud_instance.this_stratum][the_name]
        @bud_instance.delta_scanners[@bud_instance.this_stratum][the_name] = Bud::DeltaScannerElement.new(the_name, @bud_instance, self, the_schema)
        @bud_instance.push_sources[@bud_instance.this_stratum][[the_name,:delta]] = @bud_instance.delta_scanners[@bud_instance.this_stratum][the_name]
      end
      return @bud_instance.scanners[@bud_instance.this_stratum][the_name], @bud_instance.delta_scanners[@bud_instance.this_stratum][the_name]
    end

    private
    def method_missing(sym, *args, &block)
      begin
        @storage.send sym, *args, &block
      rescue
        raise NoMethodError, "no method :#{sym} in class #{self.class.name}"
      end
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


    public
    def argagg(aggname, gbkey_cols, collection)
      elem, delta_elem = to_push_elem
      gbkey_cols = gbkey_cols.map{|k| canonicalize_col(k)} unless gbkey_cols.nil?
      retval = elem.argagg(aggname,gbkey_cols,canonicalize_col(collection))
      # PushElement inherits the schema accessors from this Collection
      retval.extend @schema_access
      delta_elem.wire_to(retval)
      retval
    end

    # for each distinct value of the grouping key columns, return the items in
    # that group that have the minimum value of the attribute +col+. Note that
    # multiple tuples might be returned.
    public
    def argmin(gbkey_cols, col)
      argagg(:min, gbkey_cols, col)
    end

    # for each distinct value of the grouping key columns, return the item in
    # that group that has the maximum value of the attribute +col+. Note that
    # multiple tuples might be returned.
    public
    def argmax(gbkey_cols, col)
      argagg(:max, gbkey_cols, col)
    end

    private
    def wrap_map(j, &blk)
      if blk.nil?
        return j
      else
        return j.map(&blk)
      end
    end

    # def join(collections, *preds, &blk)
    #   # since joins are stateful, we want to allocate them once and store in this Bud instance
    #   # we ID them on their tablenames, preds, and block
    #   return wrap_map(BudJoin.new(collections, @bud_instance, preds), &blk)
    # end

    # form a collection containing all pairs of items in +self+ and items in
    # +collection+
    public
    def *(collection)
      elem1, delta1 = to_push_elem
      j = elem1.join(collection)
      # puts "wiring delta"
      delta1.wire_to(j)
      return j
      # join([self, collection])
    end

    def group(key_cols, *aggpairs, &blk)
      elem, delta1 = to_push_elem
      key_cols = key_cols.map{|k| canonicalize_col(k)} unless key_cols.nil?
      aggpairs = aggpairs.map{|ap| [ap[0], canonicalize_col(ap[1])].compact} unless aggpairs.nil?
      g = elem.group(key_cols, *aggpairs, &blk)
      delta1.wire_to(g)
      return g
    end
    
    def canonicalize_col(col)
      col.class <= Symbol ? self.send(col) : col
    end

    # alias reduce inject
    def reduce(initial, &blk)
      elem1, delta1 = to_push_elem
      red_elem = elem1.reduce(initial, &blk)
      delta1.wire_to(red_elem)
      return red_elem
    end    
    

    public
    def uniquify_tabname # :nodoc: all
      # just append current number of microseconds
      @tabname = (@tabname.to_s + Time.new.tv_usec.to_s).to_sym
    end
  end

  class BudScratch < BudCollection # :nodoc: all
  end

  class BudTemp < BudCollection # :nodoc: all
  end

  class BudChannel < BudCollection
    attr_reader :locspec_idx # :nodoc: all

    def initialize(name, bud_instance, given_schema=nil, loopback=false) # :nodoc: all
      given_schema ||= [:@address, :val]
      @is_loopback = loopback
      @locspec_idx = nil

      unless @is_loopback
        the_schema, the_key_cols = BudCollection.parse_schema(given_schema)
        the_val_cols = the_schema - the_key_cols
        @locspec_idx = remove_at_sign!(the_key_cols)
        @locspec_idx = remove_at_sign!(the_schema) if @locspec_idx.nil?
        if @locspec_idx.nil?
          raise BudError, "Missing location specifier for channel '#{name}'"
        end

        # We mutate the hash key above, so we need to recreate the hash
        # XXX: ugh, hacky
        if given_schema.respond_to? :keys
          given_schema = {the_key_cols => the_val_cols}
        end
      end

      super(name, bud_instance, given_schema)
    end

    private
    def remove_at_sign!(cols)
      i = cols.find_index {|c| c.to_s[0].chr == '@'}
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
        raise BudError, "Illegal location specifier in tuple #{t.inspect} for channel \"#{tabname}\": #{e.to_s}"
      end
    end

    public
    def clone_empty
      self.class.new(tabname, bud_instance, @given_schema, @is_loopback)
    end

    public
    def tick # :nodoc: all
      @storage = {}
      # Note that we do not clear @pending here: if the user inserted into the
      # channel manually (e.g., via <~ from inside a sync_do block), we send the
      # message at the end of the current tick.
    end

    public
    def flush # :nodoc: all
      ip = @bud_instance.ip
      port = @bud_instance.port
      each_from([@pending]) do |t|
        if @is_loopback
          the_locspec = [ip, port]
        else
          the_locspec = split_locspec(t, @locspec_idx)
          raise BudError, "'#{t[@locspec_idx]}', channel '#{@tabname}'" if the_locspec[0].nil? or the_locspec[1].nil? or the_locspec[0] == '' or the_locspec[1] == ''
        end
        @bud_instance.dsock.send_datagram([@tabname, t].to_msgpack, the_locspec[0], the_locspec[1])
      end
      @pending.clear
    end

    public
    # project to the non-address fields
    def payloads
      return self.pro if @is_loopback

      if schema.size > 2
        # bundle up each tuple's non-locspec fields into an array
        retval = case @locspec_idx
          when 0 then self.pro{|t| t[1..(t.size-1)]}
          when (schema.size - 1) then self.pro{|t| t[0..(t.size-2)]}
          else self.pro{|t| t[0..(@locspec_idx-1)] + t[@locspec_idx+1..(t.size-1)]}
        end
      else
        # just return each tuple's non-locspec field value
        retval = self.pro{|t| t[(@locspec_idx == 0) ? 1 : 0]}
      end
      return retval
    end

    superator "<~" do |o|
      if o.class <= PushElement
        o.wire_to_pending self
      else
        pending_merge(o)
      end
    end

    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with channel '#{@tabname}' on left"
    end

    undef merge

    def <=(o)
      raise BudError, "Illegal use of <= with channel '#{@tabname}' on left"
    end
  end

  class BudTerminal < BudCollection # :nodoc: all
    def initialize(name, given_schema, bud_instance, prompt=false) # :nodoc: all
      super(name, bud_instance, given_schema)
      @prompt = prompt
    end

    public
    def start_stdin_reader # :nodoc: all
      # XXX: Ugly hack. Rather than sending terminal data to EM via UDP,
      # we should add the terminal file descriptor to the EM event loop.
      @reader = Thread.new do
        begin
          while true
            out_io = get_out_io
            out_io.print("#{tabname} > ") if @prompt

            in_io = @bud_instance.options[:stdin]
            s = in_io.gets
            break if s.nil? # Hit EOF
            s = s.chomp if s
            tup = [s]

            ip = @bud_instance.ip
            port = @bud_instance.port
            EventMachine::schedule do
              socket = EventMachine::open_datagram_socket("127.0.0.1", 0)
              socket.send_datagram([tabname, tup].to_msgpack, ip, port)
            end
          end
        rescue Exception
          puts "terminal reader thread failed: #{$!}"
          print $!.backtrace.join("\n")
          exit
        end
      end
    end

    public
    def flush #:nodoc: all
      out_io = get_out_io
      @pending.each do |p|
        out_io.puts p[0]
        out_io.flush
      end
      @pending = {}
    end

    public
    def tick #:nodoc: all
      @storage = {}
      raise BudError, "orphaned pending tuples in terminal" unless @pending.empty?
    end

    undef merge

    public
    def <=(o) #:nodoc: all
      raise BudError, "Illegal use of <= with terminal '#{@tabname}' on left"
    end

    superator "<~" do |o|
      if o.class <= PushElement
        o.wire_to_pending self
      else
        pending_merge(o)
      end
    end

    private
    def get_out_io
      rv = @bud_instance.options[:stdout]
      rv ||= $stdout
      rv
    end
  end

  class BudPeriodic < BudCollection # :nodoc: all
    def <=(o)
      raise BudError, "Illegal use of <= with periodic '#{tabname}' on left"
    end

    superator "<~" do |o|
      raise BudError, "Illegal use of <~ with periodic '#{tabname}' on left"
    end

    superator "<-" do |o|
      raise BudError, "Illegal use of <- with periodic '#{tabname}' on left"
    end

    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with periodic '#{tabname}' on left"
    end

    def add_periodic_tuple(id)
      pending_merge([[id, Time.now]])
    end
  end

  class BudTable < BudCollection # :nodoc: all
    def initialize(name, bud_instance, given_schema) # :nodoc: all
      super(name, bud_instance, given_schema)
      @to_delete = []
      @to_delete_by_key = []
    end

    public
    def tick #:nodoc: all
      @to_delete.each do |tuple|
        keycols = @key_colnums.map{|k| tuple[k]}
        if @storage[keycols] == tuple
          @storage.delete keycols
        end
      end
      @to_delete_by_key.each do |tuple|
        @storage.delete @key_colnums.map{|k| tuple[k]}
      end
      @pending.each do |keycols, tuple|
        old = @storage[keycols]
        if old.nil?
          @storage[keycols] = tuple
        else
          raise_pk_error(tuple, old) unless tuple == old
        end
      end
      @to_delete = []
      @to_delete_by_key = []
      @pending = {}
    end
    
    public 
    def pending_delete(o)
      if o.class <= Bud::PushElement
         o.wire_to_delete self
       elsif o.class <= Bud::BudCollection
         o.pro.wire_to_delete self
       else
         @to_delete = @to_delete + o.map{|t| prep_tuple(t) unless t.nil?}
       end
    end
    superator "<-" do |o|
      pending_delete(o)
    end
        
    public
    def pending_delete_keys(o)
      if o.class <= Bud::PushElement
        o.wire_to_delete_by_key self
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to_delete_by_key self
      else
        @to_delete_by_key = @to_delete_by_key + o.map{|t| prep_tuple(t) unless t.nil?}
      end
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

  class BudReadOnly < BudScratch # :nodoc: all
    superator "<+" do |o|
      raise CompileError, "Illegal use of <+ with read-only collection '#{@tabname}' on left"
    end
    public
    def merge(o)  #:nodoc: all
      raise CompileError, "Illegal use of <= with read-only collection '#{@tabname}' on left"
    end
  end

  class BudFileReader < BudReadOnly # :nodoc: all
    def initialize(name, filename, delimiter, bud_instance) # :nodoc: all
      super(name, bud_instance, {[:lineno] => [:text]})
      @filename = filename
      @storage = {}
      # NEEDS A TRY/RESCUE BLOCK
      @fd = File.open(@filename, "r")
      @linenum = 0
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
      each_raw {|l| tuple_accessors(blk.call(l))}
    end
  end
end

module Enumerable
  # public
  # # monkeypatch to Enumerable to rename collections and their schemas
  # def rename(new_tabname, new_schema=nil)
  #   scr = Bud::BudScratch.new(new_tabname.to_s, nil, new_schema)
  #   scr.merge(self, scr.storage)
  #   scr
  # end

  public
  # We rewrite "map" calls in Bloom blocks to invoke the "pro" method
  # instead. This is fine when applied to a BudCollection; when applied to a
  # normal Enumerable, just treat pro as an alias for map.
  def pro(&blk)
    map(&blk)
  end
end
