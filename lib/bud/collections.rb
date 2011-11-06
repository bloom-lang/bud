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

    # This needs to be an accessor to allow REBL to update it after cloning a
    # Bud instance.
    attr_accessor :bud_instance # :nodoc: all
    attr_reader :cols, :key_cols, :tabname # :nodoc: all
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

    private
    def init_schema(given_schema)
      given_schema ||= {[:key]=>[:val]}

      # Check that no location specifiers appear in the schema. In the case of
      # channels, the location specifier has already been stripped from the
      # user-specified schema.
      given_schema.each do |s|
        if s.to_s.start_with? "@"
          raise Bud::Error, "illegal use of location specifier (@) in column #{s} of non-channel collection #{tabname}"
        end
      end

      @given_schema = given_schema
      @cols, @key_cols = parse_schema(given_schema)
      @key_colnums = key_cols.map {|k| @cols.index(k)}
      setup_accessors
    end

    # The user-specified schema might come in two forms: a hash of Array =>
    # Array (key_cols => remaining columns), or simply an Array of columns (if
    # no key_cols were specified). Return a pair: [list of (all) columns, list
    # of key columns]
    private
    def parse_schema(given_schema)
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
          raise Bud::Error, "invalid schema element \"#{c}\", type \"#{c.class}\""
        end
      end
      if cols.uniq.length < cols.length
        raise Bud::Error, "schema for #{tabname} contains duplicate names"
      end

      return [cols, key_cols]
    end

    public
    def clone_empty #:nodoc: all
      self.class.new(tabname, bud_instance, @given_schema)
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
      s = @cols
      s.each do |colname|
        reserved = eval "defined?(#{colname})"
        unless (reserved.nil? or
          (reserved == "method" and method(colname).arity == -1 and (eval(colname))[0] == self.tabname))
          raise Bud::Error, "symbol :#{colname} reserved, cannot be used as column name for #{tabname}"
        end
      end

      # set up schema accessors, which are class methods
      m = Module.new do
        s.each_with_index do |c, i|
          define_method c do
            [@tabname, i, c]
          end
        end
      end
      self.extend m

      # now set up a Module for tuple accessors, which are instance methods
      @tupaccess = Module.new do
        s.each_with_index do |colname, offset|
          define_method colname do
            self[offset]
          end
        end
      end
    end

    # define methods to access tuple attributes by column name
    private
    def tuple_accessors(tup)
      tup.extend @tupaccess
    end

    # generate a tuple with the schema of this collection and nil values in each attribute
    public
    def null_tuple
      tuple_accessors(Array.new(@cols.length))
    end

    # project the collection to its key attributes
    public
    def keys
      self.map{|t| @key_colnums.map {|i| t[i]}}
    end

    # project the collection to its non-key attributes
    public
    def values
      self.map{|t| (self.key_cols.length..self.cols.length-1).map{|i| t[i]}}
    end

    # map each item in the collection into a string, suitable for placement in stdio
    public
    def inspected
      [["#{@tabname}: [#{self.map{|t| "\n  (#{t.map{|v| v.inspect}.join ", "})"}}]"]]
    end

    # akin to map, but modified for efficiency in Bloom statements
    public
    def pro(&blk)
      if @bud_instance.stratum_first_iter
        return map(&blk)
      else
        retval = []
        each_from([@delta]) do |t|
          newitem = blk.call(t)
          retval << newitem unless newitem.nil?
        end
        return retval
      end
    end

    # By default, all tuples in any rhs are in storage or delta. Tuples in
    # new_delta will get transitioned to delta in the next iteration of the
    # evaluator (but within the current time tick).
    public
    def each(&block) # :nodoc: all
      each_from([@storage, @delta], &block)
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
          yield v
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
        else raise Bud::Error, "bad symbol passed into each_from_sym"
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
      return t.nil? ? @delta[k] : t
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
      raise KeyConstraintError, "key conflict inserting #{new_guy.inspect} into \"#{tabname}\": existing tuple #{old.inspect}, key_cols = #{keycols.inspect}"
    end

    private
    def prep_tuple(o)
      unless o.respond_to?(:length) and o.respond_to?(:[])
        raise Bud::TypeError, "non-indexable type inserted into \"#{tabname}\": #{o.inspect}"
      end
      if o.class <= String
        raise Bud::TypeError, "String value used as a fact inserted into \"#{tabname}\": #{o.inspect}"
      end

      if o.length < cols.length then
        # if this tuple has too few fields, pad with nil's
        old = o.clone
        (o.length..cols.length-1).each{|i| o << nil}
        # puts "in #{@tabname}, converted #{old.inspect} to #{o.inspect}"
      elsif o.length > cols.length then
        # if this tuple has more fields than usual, bundle up the
        # extras into an array
        o = (0..(cols.length - 1)).map{|c| o[c]} << (cols.length..(o.length - 1)).map{|c| o[c]}
      end
      return o
    end

    private
    def do_insert(o, store)
      return if o.nil? # silently ignore nils resulting from map predicates failing
      o = prep_tuple(o)
      keycols = @key_colnums.map{|i| o[i]}

      old = store[keycols]
      if old.nil?
        store[keycols] = tuple_accessors(o)
      else
        raise_pk_error(o, old) unless old == o
      end
    end

    public
    def insert(o) # :nodoc: all
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
        raise Bud::TypeError, "collection #{tabname} expected Enumerable value, not #{o.inspect} (class = #{o.class})"
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
    def merge(o, buf=@new_delta) # :nodoc: all
      unless o.nil?
        o = o.uniq if o.respond_to?(:uniq)
        check_enumerable(o)
        establish_schema(o) if @cols.nil?

        # it's a pity that we are massaging the tuples that already exist in the head
        o.each do |t|
          next if t.nil? or t == []
          t = prep_tuple(t)
          key_vals = @key_colnums.map{|k| t[k]}
          buf[key_vals] = tuple_accessors(t) unless include_any_buf?(t, key_vals)
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
      check_enumerable(o)
      establish_schema(o) if @cols.nil?

      o.each {|i| do_insert(i, @pending)}
      return self
    end

    public
    superator "<+" do |o|
      pending_merge o
    end

    public
    superator "<+-" do |o|
      self <+ o
      self <- o.map do |t|
        unless t.nil?
          self[@key_colnums.map{|k| t[k]}]
        end
      end
    end

    public
    superator "<-+" do |o|
      self <+- o
    end

    # Called at the end of each timestep: prepare the collection for the next
    # timestep.
    public
    def tick  # :nodoc: all
      @storage = @pending
      @pending = {}
      raise Bud::Error, "orphaned tuples in @delta for #{@tabname}" unless @delta.empty?
      raise Bud::Error, "orphaned tuples in @new_delta for #{@tabname}" unless @new_delta.empty?
    end

    # move deltas to storage, and new_deltas to deltas.
    public
    def tick_deltas # :nodoc: all
      # assertion: intersect(@storage, @delta) == nil
      @storage.merge!(@delta)
      @delta = @new_delta
      @new_delta = {}
    end

    private
    def method_missing(sym, *args, &block)
      @storage.send sym, *args, &block
    end

    ######## aggs

    private
    # we only do grouping during first iteration of stratum.  group and argagg should
    # never deal with deltas.  This assumes that stratification is done right, and it will
    # be sensitive to bugs in the stratification!
    def agg_in
      if not respond_to?(:bud_instance) or bud_instance.nil? or bud_instance.stratum_first_iter
        return self
      else
        return []
      end
    end

    # a generalization of argmin/argmax to arbitrary exemplary aggregates.
    # for each distinct value of the grouping key columns, return the items in that group
    # that have the value of the exemplary aggregate +aggname+
    public
    def argagg(aggname, gbkey_cols, collection)
      agg = bud_instance.send(aggname, nil)[0]
      raise Bud::Error, "#{aggname} not declared exemplary" unless agg.class <= Bud::ArgExemplary
      keynames = gbkey_cols.map do |k|
        if k.class == Symbol
          k.to_s
        else
          k[2]
        end
      end
      if collection.class == Symbol
        colnum = self.send(collection.to_s)[1]
      else
        colnum = collection[1]
      end
      tups = agg_in.inject({}) do |memo,p|
        pkey_cols = keynames.map{|n| p.send(n.to_sym)}
        if memo[pkey_cols].nil?
          memo[pkey_cols] = {:agg=>agg.send(:init, p[colnum]), :tups => [p]}
        else
          memo[pkey_cols][:agg], argflag = \
             agg.send(:trans, memo[pkey_cols][:agg], p[colnum])
          if argflag == :keep or agg.send(:tie, memo[pkey_cols][:agg], p[colnum])
            memo[pkey_cols][:tups] << p
          elsif argflag == :replace
            memo[pkey_cols][:tups] = [p]
          elsif argflag.class <= Array and argflag[0] == :delete
            memo[pkey_cols][:tups] -= argflag[1..-1]
          end
        end
        memo
      end

      # now we need to finalize the agg per group
      finalaggs = {}
      finals = []
      tups.each do |k,v|
        finalaggs[k] = agg.send(:final, v[:agg])
      end

      # and winnow the tups to match
      finalaggs.each do |k,v|
        tups[k][:tups].each do |t|
          finals << t if (t[colnum] == v)
        end
      end

      if block_given?
        finals.map{|r| yield r}
      else
        # merge directly into retval.storage, so that the temp tuples get picked up
        # by the lhs of the rule
        retval = BudScratch.new('argagg_temp', bud_instance, @given_schema)
        retval.uniquify_tabname
        retval.merge(finals, retval.storage)
      end
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

    def join(collections, *preds, &blk)
      # since joins are stateful, we want to allocate them once and store in this Bud instance
      # we ID them on their tablenames, preds, and block
      return wrap_map(BudJoin.new(collections, @bud_instance, preds), &blk)
    end

    # form a collection containing all pairs of items in +self+ and items in
    # +collection+
    public
    def *(collection)
      join([self, collection])
    end

    # AntiJoin
    public
    def notin(coll, *preds, &blk)
      return BudJoin.new([self, coll], @bud_instance).anti(*preds, &blk)
    end

    # SQL-style grouping.  first argument is an array of attributes to group by.
    # Followed by a variable-length list of aggregates over attributes (e.g. +min(:x)+)
    # Attributes can be referenced as symbols, or as +collection_name.attribute_name+
    public
    def group(key_cols, *aggpairs)
      key_cols = [] if key_cols.nil?
      keynames = key_cols.map do |k|
        if k.class == Symbol
          k
        elsif k[2] and k[2].class == Symbol
          k[2]
        else
          raise Bud::CompileError, "invalid grouping key"
        end
      end
      aggcolsdups = aggpairs.map{|ap| ap[0].class.name.split("::").last}
      aggcols = []
      aggcolsdups.each_with_index do |n, i|
        aggcols << "#{n.downcase}_#{i}".to_sym
      end
      aggpairs = aggpairs.map do |ap|
        if ap[1].class == Symbol
          colnum = ap[1].nil? ? nil : self.send(ap[1].to_s)[1]
        else
          colnum = ap[1].nil? ? nil : ap[1][1]
        end
        [ap[0], colnum]
      end
      tups = agg_in.inject({}) do |memo, p|
        pkey_cols = keynames.map{|n| p.send(n)}
        memo[pkey_cols] = [] if memo[pkey_cols].nil?
        aggpairs.each_with_index do |ap, i|
          agg = ap[0]
          colval = ap[1].nil? ? nil : p[ap[1]]
          if memo[pkey_cols][i].nil?
            memo[pkey_cols][i] = agg.send(:init, colval)
          else
            memo[pkey_cols][i], ignore = agg.send(:trans, memo[pkey_cols][i], colval)
          end
        end
        memo
      end

      result = tups.inject([]) do |memo, t|
        finals = []
        aggpairs.each_with_index do |ap, i|
          finals << ap[0].send(:final, t[1][i])
        end
        memo << t[0] + finals
      end
      if block_given?
        result.map{|r| yield r}
      else
        # merge directly into retval.storage, so that the temp tuples get picked up
        # by the lhs of the rule
        if aggcols.empty?
          schema = keynames
        else
          schema = { keynames => aggcols }
        end
        retval = BudScratch.new('temp_group', bud_instance, schema)
        retval.uniquify_tabname
        retval.merge(result, retval.storage)
      end
    end

    alias reduce inject

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

      # We're going to mutate the caller's given_schema (to remove the location
      # specifier), so make a deep copy first. We also save a ref to the
      # unmodified given_schema.
      @raw_schema = given_schema
      given_schema = Marshal.load(Marshal.dump(given_schema))

      unless @is_loopback
        the_cols, the_key_cols = parse_schema(given_schema)
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
        raise Bud::Error, "illegal location specifier in tuple #{t.inspect} for channel \"#{tabname}\": #{e.to_s}"
      end
    end

    public
    def clone_empty
      self.class.new(tabname, bud_instance, @raw_schema, @is_loopback)
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
          raise Bud::Error, "'#{t[@locspec_idx]}', channel '#{@tabname}'" if the_locspec[0].nil? or the_locspec[1].nil? or the_locspec[0] == '' or the_locspec[1] == ''
        end
        @bud_instance.dsock.send_datagram([@tabname, t].to_msgpack, the_locspec[0], the_locspec[1])
      end
      @pending.clear
    end

    public
    # project to the non-address fields
    def payloads
      return self.pro if @is_loopback

      if cols.size > 2
        # bundle up each tuple's non-locspec fields into an array
        retval = case @locspec_idx
          when 0 then self.pro{|t| t[1..(t.size-1)]}
          when (cols.size - 1) then self.pro{|t| t[0..(t.size-2)]}
          else self.pro{|t| t[0..(@locspec_idx-1)] + t[@locspec_idx+1..(t.size-1)]}
        end
      else
        # just return each tuple's non-locspec field value
        retval = self.pro{|t| t[(@locspec_idx == 0) ? 1 : 0]}
      end
      return retval
    end

    superator "<~" do |o|
      pending_merge o
    end

    superator "<+" do |o|
      raise Bud::Error, "illegal use of <+ with channel '#{@tabname}' on left"
    end

    undef merge

    def <=(o)
      raise Bud::Error, "illegal use of <= with channel '#{@tabname}' on left"
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
      raise Bud::Error, "orphaned pending tuples in terminal" unless @pending.empty?
    end

    undef merge

    public
    def <=(o) #:nodoc: all
      raise Bud::Error, "illegal use of <= with terminal '#{@tabname}' on left"
    end

    superator "<~" do |o|
      pending_merge(o)
    end

    private
    def get_out_io
      rv = @bud_instance.options[:stdout]
      rv ||= $stdout
      raise Bud::Error, "attempting to write to terminal #{tabname} that was already closed" if rv.closed?
      rv
    end
  end

  class BudPeriodic < BudCollection # :nodoc: all
    def <=(o)
      raise Bud::Error, "illegal use of <= with periodic '#{tabname}' on left"
    end

    superator "<~" do |o|
      raise Bud::Error, "illegal use of <~ with periodic '#{tabname}' on left"
    end

    superator "<-" do |o|
      raise Bud::Error, "illegal use of <- with periodic '#{tabname}' on left"
    end

    superator "<+" do |o|
      raise Bud::Error, "illegal use of <+ with periodic '#{tabname}' on left"
    end
  end

  class BudTable < BudCollection # :nodoc: all
    def initialize(name, bud_instance, given_schema) # :nodoc: all
      super(name, bud_instance, given_schema)
      @to_delete = []
    end

    public
    def tick #:nodoc: all
      @to_delete.each do |tuple|
        keycols = @key_colnums.map{|k| tuple[k]}
        if @storage[keycols] == tuple
          @storage.delete keycols
        end
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
      @pending = {}
    end

    superator "<-" do |o|
      o.each do |t|
        next if t.nil?
        @to_delete << prep_tuple(t)
      end
    end
  end

  class BudReadOnly < BudScratch # :nodoc: all
    superator "<+" do |o|
      raise CompileError, "illegal use of <+ with read-only collection '#{@tabname}' on left"
    end
    public
    def merge(o)  #:nodoc: all
      raise CompileError, "illegal use of <= with read-only collection '#{@tabname}' on left"
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
    def pro(&blk)
      if @bud_instance.stratum_first_iter
        return map(&blk)
      else
        return []
      end
    end

    public
    def each(&block) # :nodoc: all
      while (l = @fd.gets)
        t = tuple_accessors([@linenum, l.strip])
        @linenum += 1
        tick_metrics if bud_instance.options[:metrics]
        yield t
      end
    end
  end
end

module Enumerable
  public
  # monkeypatch to Enumerable to rename collections and their schemas
  def rename(new_tabname, new_schema=nil)
    budi = (respond_to?(:bud_instance)) ? bud_instance : nil
    if new_schema.nil? and respond_to?(:schema)
      new_schema = schema
    end
    scr = Bud::BudScratch.new(new_tabname.to_s, budi, new_schema)
    scr.uniquify_tabname
    scr.merge(self, scr.storage)
    scr
  end

  public
  # We rewrite "map" calls in Bloom blocks to invoke the "pro" method
  # instead. This is fine when applied to a BudCollection; when applied to a
  # normal Enumerable, just treat pro as an alias for map.
  def pro(&blk)
    map(&blk)
  end
end
