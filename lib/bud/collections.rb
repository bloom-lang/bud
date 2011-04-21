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

    attr_accessor :bud_instance, :locspec_idx # :nodoc: all
    attr_reader :schema, :tabname # :nodoc: all
    attr_reader :storage, :delta, :new_delta # :nodoc: all

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
      @given_schema = given_schema
      @schema, @key_cols = parse_schema(given_schema)
      @key_colnums = key_cols.map {|k| schema.index(k)}
      setup_accessors
    end

    # The user-specified schema might come in two forms: a hash of Array =>
    # Array (key_cols => remaining columns), or simply an Array of columns (if no
    # key_cols were specified). Return a pair: [list of columns in entire tuple,
    # list of key columns]
    private
    def parse_schema(given_schema)
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
        raise BudError, "schema for #{tabname} contains duplicate names"
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
      s = @schema
      s.each do |colname|
        reserved = eval "defined?(#{colname})"
        unless (reserved.nil? or
          (reserved == "method" and method(colname).arity == -1 and (eval(colname))[0] == self.tabname))
          raise BudError, "symbol :#{colname} reserved, cannot be used as column name for #{tabname}"
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
      tuple_accessors(Array.new(@schema.length))
    end

    # project the collection to its key attributes
    public
    def keys
      self.map{|t| (0..self.key_cols.length-1).map{|i| t[i]}}
    end

    # project the collection to its non-key attributes
    public
    def values
      self.map{|t| (self.key_cols.length..self.schema.length-1).map{|i| t[i]}}
    end

    # map each item in the collection into a string, suitable for placement in stdio
    public
    def inspected
      self.map{|t| [t.inspect]}
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

    private
    def each_from(bufs, &block) # :nodoc: all
      bufs.each do |b|
        b.each_value do |v|
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
      return false if k.nil? or k.empty? or self[k].nil?
      return true
    end

    # return item with key +k+
    public
    def [](k)
      # assumes that key is in storage or delta, but not both
      # is this enforced in do_insert?
      return @storage[k].nil? ? @delta[k] : @storage[k]
    end

    # checks for +item+ in the collection
    public
    def include?(item)
      return true if key_cols.nil? or (key_cols.empty? and length > 0)
      return false if item.nil? or item.empty?
      key = key_cols.map{|k| item[schema.index(k)]}
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
    def raise_pk_error(new, old)
      keycols = key_cols.map{|k| old[schema.index(k)]}
      raise KeyConstraintError, "Key conflict inserting #{old.inspect} into \"#{tabname}\": existing tuple #{new.inspect}, key_cols = #{keycols.inspect}"
    end

    private
    def prep_tuple(o)
      unless o.respond_to?(:length) and o.respond_to?(:[])
        raise BudTypeError, "non-indexable type inserted into BudCollection #{self.tabname}: #{o.inspect}"
      end

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
      unless (o.nil? or o.class < Enumerable) and o.respond_to? 'each'
        raise BudTypeError, "Attempt to merge non-enumerable type into BudCollection"
      end
    end

    # Assign self a schema, by hook or by crook.  If +o+ is schemaless *and*
    # empty, will leave @schema as is.
    private
    def establish_schema(o)
      # use o's schema if available
      deduce_schema(o) if @schema.nil?
      # else use arity of first tuple of o
      fit_schema(o.first.size) if @schema.nil? and not o.first.nil?
      return @schema
    end

    # Copy over the schema from +o+ if available
    private
    def deduce_schema(o)
      if @schema.nil? and o.class <= Bud::BudCollection and not o.schema.nil?
        # must have been initialized with defer_schema==true.  take schema from rhs
        init_schema(o.schema)
      end
      # returns old state of @schema (nil) if nothing available
      return @schema
    end

    # manufacture schema of the form [:c0, :c1, ...] with width = +arity+
    private
    def fit_schema(arity)
      # rhs is schemaless.  create schema from first tuple merged
      init_schema((0..arity-1).map{|indx| ("c"+indx.to_s).to_sym})
      return @schema
    end

    public
    def merge(o, buf=@new_delta) # :nodoc: all
      check_enumerable(o)
      establish_schema(o) if @schema.nil?

      delta = o.map do |i|
        next if i.nil? or i == []
        i = prep_tuple(i)
        key_vals = @key_colnums.map{|k| i[k]}
        if (old = self[key_vals])
          raise_pk_error(i, old) if old != i
        elsif (oldnew = self.new_delta[key_vals])
          raise_pk_error(i, oldnew) if oldnew != i
        else
          buf[key_vals] = tuple_accessors(i)
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
      deduce_schema(o)

      o.each {|i| do_insert(i, @pending)}
      return self
    end

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
    # for each distinct value in the grouping key columns, return the item in that group
    # that has the value of the exemplary aggregate +aggname+
    public
    def argagg(aggname, gbkey_cols, collection)
      agg = bud_instance.send(aggname, nil)[0]
      raise BudError, "#{aggname} not declared exemplary" unless agg.class <= Bud::ArgExemplary
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
          newval = agg.send(:trans, memo[pkey_cols][:agg], p[colnum])
          if memo[pkey_cols][:agg] == newval
            if agg.send(:tie, memo[pkey_cols][:agg], p[colnum])
              memo[pkey_cols][:tups] << p
            end
          else
            memo[pkey_cols] = {:agg=>newval, :tups=>[p]}
          end
        end
        memo
      end

      finals = []
      outs = tups.each_value do |t|
        ties = t[:tups].map do |tie|
          finals << tie
        end
      end

      # merge directly into retval.storage, so that the temp tuples get picked up
      # by the lhs of the rule
      retval = BudScratch.new('argagg_temp', bud_instance, @given_schema)
      retval.uniquify_tabname
      retval.merge(finals, retval.storage)
    end

    # for each distinct value in the grouping key columns, return the item in that group
    # that has the minimum value of the attribute +col+
    public
    def argmin(gbkey_cols, col)
      argagg(:min, gbkey_cols, col)
    end

    # for each distinct value in the grouping key columns, return the item in that group
    # that has the maximum value of the attribute +col+
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
    def *(collection, &blk)
      join([self, collection])
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
          raise Bud::CompileError, "Invalid grouping key"
        end
      end
      aggcolsdups = aggpairs.map{|ap| ap[0].class.name.split("::").last}
      aggcols = []
      aggcolsdups.each_with_index do |n, i|
        aggcols << "#{n.downcase}_#{i}".to_sym
      end
      tups = agg_in.inject({}) do |memo, p|
        pkey_cols = keynames.map{|n| p.send(n)}
        memo[pkey_cols] = [] if memo[pkey_cols].nil?
        aggpairs.each_with_index do |ap, i|
          agg = ap[0]
          if ap[1].class == Symbol
            colnum = ap[1].nil? ? nil : self.send(ap[1].to_s)[1]
          else
            colnum = ap[1].nil? ? nil : ap[1][1]
          end
          colval = colnum.nil? ? nil : p[colnum]
          if memo[pkey_cols][i].nil?
            memo[pkey_cols][i] = agg.send(:init, colval)
          else
            memo[pkey_cols][i] = agg.send(:trans, memo[pkey_cols][i], colval)
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

    # methods that work on nested collections (resulting from joins)


    # given a * expression over n collections, form all combinations of items
    # subject to an array of predicates, pred
    # currently supports two options for equijoin predicates:
    #    general form: an array of arrays capturing a conjunction of equiv. classes
    #          [[table1.col1, table2.col2, table3.col3], [table1.col2, table2.col3]]
    #    common form: a hash capturing equality of a column on left with one on right.
    #          :col1 => :col2  (same as  lefttable.col1 => righttable.col2)
    public
    def pairs(*preds, &blk)
      @origpreds = preds
      setup_preds(preds) unless (preds.nil? or preds.empty?)
      # given new preds, the state for the join will be different.  set it up again.
      setup_state if self.class <= Bud::BudJoin
      blk.nil? ? self : map(&blk)
    end

    alias combos pairs

    # the natural join: given a * expression over 2 collections, form all
    # combinations of items that have the same values in matching fiels
    public
    def matches(&blk)
      preds = BudJoin::natural_preds(@bud_instance, @origrels)
      pairs(*preds, &blk)
    end

    # given a * expression over 2 collections, form all
    # combinations of items that satisfy the predicates +preds+,
    # and project only onto the attributes of the first collection
    public
    def lefts(*preds)
      unless preds.empty?
        @localpreds ||= []
        @localpreds += disambiguate_preds(preds)
      end
      map{ |l,r| l }
    end

    # given a * expression over 2 collections, form all
    # combinations of items that satisfy the predicates +preds+,
    # and project only onto the attributes of the second item
    public
    def rights(*preds)
      unless preds.empty?
        @localpreds ||= []
        @localpreds += disambiguate_preds(preds)
      end
      map{ |l,r| r }
    end

    # given a * expression over 2 collections, form all
    # combos of items that satisfy +preds+, and for any
    # item from the 1st collection that has no matches
    # in the 2nd, nil-pad it and include it in the output.
    public
    def outer(*preds)
      @origpreds = preds
      @localpreds = disambiguate_preds(preds)
      self.extend(Bud::BudOuterJoin)
      map
    end

    # extract predicates on rellist[0] and recurse to right side with remainder
    protected
    def setup_preds(preds) # :nodoc: all
      allpreds = disambiguate_preds(preds)
      allpreds = canonicalize_localpreds(@rels, allpreds)
      @localpreds = allpreds.reject { |p|  p[0][0] != @rels[0].tabname }
      otherpreds = allpreds - @localpreds
      otherpreds = nil if otherpreds.empty?
      unless otherpreds.nil?
        unless @rels[1].class <= Bud::BudJoin
          raise BudError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
        end
        @rels[1].setup_preds(otherpreds)
      end
    end

    protected
    def disambiguate_preds(preds) # :nodoc: all
      if preds.size == 1 and preds[0].class <= Hash
        predarray = preds[0].map do |k,v|
          if k.class != v.class
            raise Bud::CompileError, "inconsistent attribute ref style #{k.inspect} => #{v.inspect}"
          elsif k.class <= Array
            [k,v]
          elsif k.class <= Symbol
            if @origrels and @origrels.length == 2
              [find_attr_match(k,@origrels[0]), find_attr_match(v,@origrels[1])]
            else
              [find_attr_match(k), find_attr_match(v)]
            end
          else
            raise Bud::CompileError, "invalid attribute ref in #{k.inspect} => #{v.inspect}"
          end
        end
        return decomp_preds(*predarray)
      else
        return decomp_preds(*preds)
      end
    end

    # find element in @origrels that contains this aname method
    # if 2nd arg is non-nil, only check that collection.
    # after found, return the result of invoking aname from chosen collection
    protected
    def find_attr_match(aname, rel=nil) # :nodoc: all
      dorels = (rel.nil? ? @origrels : [rel])
      match = nil
      dorels.each do |r|
        match ||= r if r.respond_to?(aname)
        if r.respond_to?(aname) and match != r
          raise Bud::CompileError, "ambiguous attribute :#{aname} in both #{match.tabname} and #{r.tabname}"
        end
      end
      if match.nil?
        raise Bud::CompileError, "attribute :#{aname} not found in any of #{dorels.map{|t| t.tabname}.inspect}"
      end
      match.send(aname)
    end

    protected
    def decomp_preds(*preds) # :nodoc:all
      # decompose each pred into a binary pred
      return nil if preds.nil? or preds.empty? or preds == [nil]
      newpreds = []
      preds.each do |p|
        p.each_with_index do |c, i|
          newpreds << [p[i], p[i+1]] unless p[i+1].nil?
        end
      end
      newpreds
    end

    protected
    def canonicalize_localpreds(rel_list, preds) # :nodoc:all
      return if preds.nil?
      retval = preds.map do |p|
        p[1][0] == rel_list[0].tabname ? p.reverse : p
      end
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

    def initialize(name, bud_instance, given_schema=nil) # :nodoc: all
      given_schema ||= [:@address, :val]
      the_schema, the_key_cols = parse_schema(given_schema)
      the_val_cols = the_schema - the_key_cols
      @locspec_idx = remove_at_sign!(the_key_cols)
      @locspec_idx = remove_at_sign!(the_schema) if @locspec_idx.nil?
      # If @locspec_idx is still nil, this is a loopback channel

      # We mutate the hash key above, so we need to recreate the hash
      # XXX: ugh, hacky
      if given_schema.respond_to? :keys
        given_schema = {the_key_cols => the_val_cols}
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
    def split_locspec(l)
      lsplit = l.split(':')
      lsplit[1] = lsplit[1].to_i
      return lsplit
    end

    public
    def clone_empty
      retval = super
      retval.locspec_idx = @locspec_idx
      retval
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
        if @locspec_idx.nil?
          the_locspec = [ip, port]
        else
          the_locspec = split_locspec(t[@locspec_idx])
          raise BudError, "'#{t[@locspec_idx]}', channel '#{@tabname}'" if the_locspec[0].nil? or the_locspec[1].nil? or the_locspec[0] == '' or the_locspec[1] == ''
        end
        @bud_instance.dsock.send_datagram([@tabname, t].to_msgpack, the_locspec[0], the_locspec[1])
      end
      @pending.clear
    end

    public
    # project to the non-address fields
    def payloads
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
      pending_merge o
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
            $stdout.print("#{tabname} > ") if @prompt
            s = $stdin.gets
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
        rescue
          puts "terminal reader thread failed: #{$!}"
          print $!.backtrace.join("\n")
          exit
        end
      end
    end

    public
    def flush #:nodoc: all
      @pending.each do |p|
        $stdout.puts p[0]
        $stdout.flush
      end
      @pending = {}
    end

    public
    def tick #:nodoc: all
      @storage = {}
      raise BudError unless @pending.empty?
    end

    undef merge

    public
    def <=(o) #:nodoc: all
      raise BudError, "Illegal use of <= with terminal '#{@tabname}' on left"
    end

    superator "<~" do |o|
      pending_merge(o)
    end
  end

  class BudPeriodic < BudCollection # :nodoc: all
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
      @storage.merge! @pending
      @to_delete = []
      @pending = {}
    end

    superator "<-" do |o|
      o.each do |tuple|
        next if tuple.nil?
        tuple = prep_tuple(tuple)
        @to_delete << tuple
      end
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
end
