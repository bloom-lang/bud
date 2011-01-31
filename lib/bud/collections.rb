require 'tokyocabinet'
require 'zookeeper'

class Bud
  ######## the collection types
  class BudCollection
    include Enumerable

    attr_accessor :schema, :keys, :cols
    attr_reader :tabname, :bud_instance, :storage, :delta, :new_delta

    # each collection is partitioned into 4:
    # - pending holds tuples deferred til the next tick
    # - storage holds the "normal" tuples
    # - delta holds the delta for rhs's of rules during semi-naive
    # - new_delta will hold the lhs tuples currently being produced during s-n
    def initialize(name, keys, cols, bud_instance)
      @tabname = name
      @schema = keys+cols
      @keys = keys
      init_storage
      init_pending
      init_deltas
      @bud_instance = bud_instance
      raise BudError, "schema for #{tabname} contains duplicate names" if schema.uniq.length < schema.length
      setup_accessors
    end

    def clone_empty
      self.class.new(tabname, keys, cols, bud_instance)
    end

    def cols
      schema - keys
    end

    def tick
      @storage = @pending
      @pending = {}
      @delta = {}
      @new_delta = {}
      self
    end

    # define methods to turn 'table.col' into a [table,col] pair
    # e.g. to support something like
    #    j = join link, path, {link.to => path.from}
    def setup_accessors
      s = @schema
      s.each do |colname|
        reserved = eval "defined?(#{colname})"
        unless (reserved.nil? or 
                (reserved == "method" and method(colname).arity == -1 and (eval(colname))[0] == self.tabname))
          raise Bud::BudError, "symbol :#{colname} reserved, cannot be used as column name for #{tabname}"
        end
      end
  
      # set up schema accessors, which are class methods
      m = Module.new do
        s.each_with_index do |c, i|
          define_method c.to_sym do
            [@tabname, i, c]
          end
        end
      end
      self.extend m
      
      # now set up a Module for tuple accessors, which are instance methods
      @tupaccess = Module.new do
        s.each_with_index do |colname, offset|
          define_method colname.to_sym do
            self[offset]
          end
        end
      end
    end

    # def tuple_accessor(tup, colname, offset)
    #   unless tup.respond_to? colname.to_sym
    #     tup.extend @tupaccess
    #   end
    # end

    # define methods to access tuple attributes by column name
    # paa: inverted loop to add test, fix leak
    def tuple_accessors(tup)
      tup.extend @tupaccess
    end

    def null_tuple
      return tuple_accessors(@schema.map{|c| nil})
    end
    
    # by default, all tuples in any rhs are in storage or delta
    # tuples in new_delta will get transitioned to delta in the next
    # iteration of the evaluator (but within the current time tick)
    def each(&block)
      each_from([@storage, @delta], &block)
    end

    def each_from(bufs, &block)
      bufs.each do |b|
        b.each_value do |v|
          # @bud_instance.each_counter[tabname] ||= 0 unless @bud_instance.nil?
          # @bud_instance.each_counter[tabname] += 1  unless @bud_instance.nil?
          yield v
        end
      end
    end

    def each_storage(&block)
      each_from([@storage], &block)
    end

    def each_pending(&block)
      each_from([@pending], &block)
    end

    def each_delta(&block)
      each_from([@delta], &block)
    end

    def each_new_delta(&block)
      each_from([@new_delta], &block)
    end

    def init_storage
      @storage = {}
    end

    def init_pending
      @pending = {}
    end

    def init_deltas
      @delta = {}
      @new_delta = {}
    end

    def include?(o)
      return false if o.nil? or o.length == 0
      return (self[o] == o)
    end

    def raise_pk_error(new, old)
      keycols = keys.map{|k| old[schema.index(k)]}
      raise KeyConstraintError, "Key conflict inserting #{old.inspect} into \"#{tabname}\": existing tuple #{new.inspect}, keys = #{keycols.inspect}"
    end

    def do_insert(o, store)
      return if o.nil? or o.length == 0

      keycols = keys.map{|k| o[schema.index(k)]}
      old = store[keycols]
      raise_pk_error(o, old) unless old.nil? or old == o

      store[keycols] = tuple_accessors(o)
    end

    def insert(o)
      # puts "insert: #{o.inspect} into #{tabname}"
      do_insert(o, @storage)
    end

    alias << insert

    def pending_insert(o)
      # puts "pending_insert: #{o.inspect} into #{tabname}"
      do_insert(o, @pending)
    end

    def merge(o, buf=@new_delta)
      raise BudError, "Attempt to merge non-enumerable type into BloomCollection: #{o.inspect}" unless o.respond_to? 'each'
      delta = o.map do |i| 
        next if i.nil? or i == []
        keycols = keys.map{|k| i[schema.index(k)]}
        if (old = self[keycols])
          raise_pk_error(i, old) if old != i
        elsif (oldnew = self.new_delta[keycols])
          raise_pk_error(i, oldnew) if oldnew != i
        else
          # don't call do_insert, it will just recheck our tests for hash collision
          buf[keycols] = tuple_accessors(i)
        end
      end
      if self.schema.empty? and o.respond_to?(:schema) and not o.schema.empty?
        self.schema = o.schema
      end
      return self
    end
    
    alias <= merge
    
    def pending_merge(o)
      delta = o.map {|i| self.pending_insert(i)}
      if self.schema.empty? and o.respond_to?(:schema) and not o.schema.empty?
        self.schema = o.schema
      end
      @pending
    end

    superator "<+" do |o|
      pending_merge o
    end

    # move all deltas and new_deltas into storage
    def install_deltas
      # assertion: intersect(@storage, @delta, @new_delta) == nil
      @storage.merge!(@delta)
      @storage.merge!(@new_delta)
      @delta = {}
      @new_delta = {}
    end

    # move deltas to storage, and new_deltas to deltas.
    def tick_deltas
      # assertion: intersect(@storage, @delta) == nil
      @storage.merge!(@delta)
      @delta = @new_delta
      @new_delta = {}
    end

    def [](key)
      return @storage[key].nil? ? @delta[key] : @storage[key]
    end

    def method_missing(sym, *args, &block)
      @storage.send sym, *args, &block
    end

    ######## aggs
    # currently support two options for column ref syntax -- :colname or table.colname
    def argagg(aggname, gbkeys, col)
      agg = bud_instance.send(aggname, nil)[0]
      raise BudError, "#{aggname} not declared exemplary" unless agg.class <= Bud::ArgExemplary
      #keynames = gbkeys.map {|k| k[2]}
      keynames = gbkeys.map do |k| 
        if k.class == Symbol
          k.to_s
        else
          k[2]
        end
      end
      if col.class == Symbol
        colnum = self.send(col.to_s)[1]
      else
        colnum = col[1]
      end
      tups = self.inject({}) do |memo,p|
        pkeys = keynames.map{|n| p.send(n.to_sym)}
        if memo[pkeys].nil?
          memo[pkeys] = {:agg=>agg.send(:init, p[colnum]), :tups => [p]}
        else
          newval = agg.send(:trans, memo[pkeys][:agg], p[colnum])
          if memo[pkeys][:agg] == newval
            if agg.send(:tie, memo[pkeys][:agg], p[colnum])
              memo[pkeys][:tups] << p
            end
          else
            memo[pkeys] = {:agg=>newval, :tups=>[p]}
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
      #retval.merge(finals)
      if block_given?
        finals.map{|r| yield r}      
      else
        # merge directly into retval.storage, so that the temp tuples get picked up
        # by the lhs of the rule
        retval = BudScratch.new('argagg_temp', @schema, [], bud_instance)
        retval.merge(finals, retval.storage)
      end
    end

    def argmin(gbkeys, col)
      argagg(:min, gbkeys, col)
    end

    def argmax(gbkeys, col)
      argagg(:max, gbkeys, col)
    end

    # currently support two options for column ref syntax -- :colname or table.colname
    def group(keys, *aggpairs)
      keys = [] if keys.nil?
      keynames = keys.map do |k| 
        if k.class == Symbol
          k.to_s
        else
          k[2]
        end
      end
      aggcolsdups = aggpairs.map{|ap| ap[0].class.name.split("::").last}
      aggcols = []
      aggcolsdups.each_with_index do |n,i|
        aggcols << "#{n.downcase}_#{i}"
      end
      tups = self.inject({}) do |memo,p|
        pkeys = keynames.map{|n| p.send(n.to_sym)}
#        pkeys = keys.map{|n| p.send(n)}
        memo[pkeys] = [] if memo[pkeys].nil?
        aggpairs.each_with_index do |ap, i|
          agg = ap[0]
          if ap[1].class == Symbol
            colnum = ap[1].nil? ? nil : self.send(ap[1].to_s)[1]
          else
            colnum = ap[1].nil? ? nil : ap[1][1]
          end
          colval = colnum.nil? ? nil : p[colnum]
          if memo[pkeys][i].nil?
            memo[pkeys][i] = agg.send(:init, colval)
          else
            memo[pkeys][i] = agg.send(:trans, memo[pkeys][i], colval)
          end
        end
        memo
      end

      result = tups.inject([]) do |memo,t|
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
        retval = BudScratch.new('temp', keynames, aggcols, bud_instance)        
        retval.merge(result, retval.storage)
      end
    end

    def dump
      puts '(empty)' if @storage.length == 0
      @storage.sort.each do |t|
        puts t.inspect unless cols.empty?
        puts t[0].inspect if cols.empty?
      end
      true
    end

    alias reduce inject
  end

  class BudScratch < BudCollection
  end
  
  class BudSerializer < BudCollection
    def initialize(name, keys, cols, bud_instance)
      @dq = {}
      super
    end

    def tick
      @dq.each_key {|k| @storage.delete k}
    end

    def each
      @storage.keys.sort.each do |k|
        tup = (@storage[k] == true) ? k : @storage[k]
        yield tup
        @dq[k] = true
        return
      end
    end
  end

  class BudChannel < BudCollection
    attr_accessor :locspec, :connections

    def initialize(name, keys, cols, locspec, bud_instance)
      super(name, keys, cols, bud_instance)
      @locspec = locspec
      @connections = {}
    end

    def split_locspec(l)
      lsplit = l.split(':')
      lsplit[1] = lsplit[1].to_i
      return lsplit
    end

    def clone_empty
      retval = super
      retval.locspec = locspec
      retval.connections = @connections.clone
      retval
    end

    def establish_connection(l)
      @connections[l] = EventMachine::connect l[0], l[1], BudServer, @bud_instance
    end

    def tick
      @storage = {}
      # never turn pending outbounds into real tuples
      @pending = {}
    end

    def flush
      ip = @bud_instance.ip
      port = @bud_instance.port
      each_pending do |t|
        if @locspec.nil?
          the_locspec = [ip, port.to_i]
        else
          begin
            the_locspec = split_locspec(t[@locspec])
          rescue
            puts "bad locspec #{@locspec} for #{@tabname}"
          end
        end
        establish_connection(the_locspec) if @connections[the_locspec].nil?
        @connections[the_locspec].send_data [@tabname, t].to_msgpack
      end
      @pending.clear
    end

    superator "<~" do |o|
      pending_merge o
    end

    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with channel on left"
    end
  end

  class BudTerminal < BudCollection
    def initialize(name, keys, cols, bud_instance, prompt=false)
      super(name, keys, cols, bud_instance)
      @connection = nil
      @prompt = prompt

      start_stdin_reader if bud_instance.options[:read_stdin]
    end

    def start_stdin_reader
      # XXX: Ugly hack. Rather than sending terminal data to EM via TCP,
      # we should add the terminal file descriptor to the EM event loop.
      @reader = Thread.new() do
        begin
          while true
            STDOUT.print("#{tabname} > ") if @prompt
            s = STDIN.gets
            s = s.chomp if s
            tup = tuple_accessors([s])

            ip = @bud_instance.ip
            port = @bud_instance.port
            @connection ||= EventMachine::connect ip, port, BudServer, @bud_instance
            @connection.send_data [tabname, tup].to_msgpack
          end
        rescue
          puts "terminal reader thread failed: #{$!}"
          print $!.backtrace.join("\n")
          exit
        end
      end
    end

    def flush
      @pending.each do |p|
        STDOUT.puts p[0]
      end
      @pending = {}
    end

    def tick
      @storage = {}
      @pending = {}
    end

    def merge(o)
      raise BudError, "no synchronous accumulation into terminal; use <~"
    end

    def <=(o)
      merge(o)
    end

    superator "<~" do |o|
      pending_merge(o)
    end
  end

  class BudPeriodic < BudCollection
  end

  class BudTable < BudCollection
    def initialize(name, keys, cols, bud_instance)
      super(name, keys, cols, bud_instance)
      @to_delete = []
    end

    def clone_empty
      self.class.new(name, keys, cols, bud_instance)
    end

    def tick
      @to_delete.each do |tuple|
        keycols = keys.map{|k| tuple[schema.index(k)]}
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
        @to_delete << tuple
      end
    end
  end

  class BudJoin < BudCollection
    attr_accessor :rels, :origrels

    def initialize(rellist, bud_instance, preds=nil)
      @schema = []
      otherpreds = nil
      @origrels = rellist
      @bud_instance = bud_instance

      # extract predicates on rellist[0] and let the rest recurse
      unless preds.nil?
        @localpreds = preds.reject { |p| p[0][0] != rellist[0].tabname and p[1][0] != rellist[0].tabname }
        @localpreds.each do |p|
          if p[1][0] == rellist[0].tabname
            @localpreds.delete(p)
            @localpreds << [p[1], p[0]]
          end
        end
        otherpreds = preds.reject { |p| p[0][0] == rellist[0].tabname or p[1][0] == rellist[0].tabname}
        otherpreds = nil if otherpreds.empty?
      end
      if rellist.length == 2 and not otherpreds.nil?
        raise BudError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
      end

      # recurse to form a tree of binary BudJoins
      @rels = [rellist[0]]
      @rels << (rellist.length == 2 ? rellist[1] : BudJoin.new(rellist[1..rellist.length-1], @bud_instance, otherpreds))

      # now derive schema: combo of rels[0] and rels[1]
      if @rels[0].schema.empty? or @rels[1].schema.empty?
        @schema = []
      else
        dups = @rels[0].schema & @rels[1].schema
        bothschema = @rels[0].schema + @rels[1].schema
        @schema = bothschema.to_enum(:each_with_index).map do |c,i| 
          if dups.include?(c) then 
            c + '_' + i.to_s else c 
          end
        end
      end
    end

    def do_insert(o, store)
      raise BudError, "no insertion into joins"
    end

    def each(mode=:both, &block)
      mode = :storage if @bud_instance.stratum_first_iter
      if mode == :storage
        methods = [:storage]
      else
        methods = [:delta, :storage]
      end

      methods.each do |collection1|
        methods.each do |collection2|
          next if (mode == :delta and collection1 == :storage and collection2 == :storage)
          if @localpreds.nil? or @localpreds.empty?
            nestloop_join(collection1, collection2, &block)
          else
            hash_join(collection1, collection2, &block)
          end
        end
      end
    end

    def each_storage(&block)
      return each(:storage, &block)
    end
    
    # this needs to be made more efficient!
    def each_delta(&block)
      return each(:delta, &block)
    end

    def test_locals(r, s, *skips)
      retval = true
      if (@localpreds and skips and @localpreds.length > skips.length)
        # check remainder of the predicates
        @localpreds.each do |pred|
          next if skips.include? pred
          r_offset, s_index, s_offset = join_offsets(pred)
          if r[r_offset] != s[s_index][s_offset]
            retval = false
            break
          end
        end
      end
      return retval
    end

    def nestloop_join(collection1, collection2, &block)
      @rels[0].send(('each_' + collection1.to_s).to_sym) do |r|
        @rels[1].send(('each_' + collection2.to_s).to_sym) do |s|
          s = [s] if origrels.length == 2
          yield([r] + s) if test_locals(r, s)
        end
      end
    end
    
    def join_offsets(pred)
      build_entry = pred[1]
      build_name, build_offset = build_entry[0], build_entry[1]
      probe_entry = pred[0]
      probe_name, probe_offset = probe_entry[0], probe_entry[1]

      # determine which subtuple of s contains the table referenced in RHS of pred
      # note that s doesn't contain the first entry in rels, which is r
      index = 0
      origrels[1..origrels.length].each_with_index do |t,i|
        if t.tabname == pred[1][0]
          index = i
          break
        end
      end

      return probe_offset, index, build_offset
    end

    def hash_join(collection1, collection2, &block)
      # hash join on first predicate!
      ht = {}

      probe_offset, build_tup, build_offset = join_offsets(@localpreds.first)

      # build the hashtable on s!
      rels[1].send(('each_' + collection2.to_s).to_sym) do |s|
        s = [s] if origrels.length == 2
        attrval = s[build_tup][build_offset]
        ht[attrval] ||= []
        ht[attrval] << s
      end

      # probe the hashtable!
      rels[0].send(('each_' + collection1.to_s).to_sym) do |r|
        next if ht[r[probe_offset]].nil?
        ht[r[probe_offset]].each do |s|
          retval = [r] + s
          yield(retval) if test_locals(r, s, @localpreds.first)
        end
      end
    end
  end

  class BudLeftJoin < BudJoin
    def initialize(rellist, bud_instance, preds=nil)
      raise(BudError, "Left Join only defined for two relations") unless rellist.length == 2
      super(rellist, bud_instance, preds)
      @origpreds = preds
    end

    def each(&block)
      super(&block)
      # previous line finds all the matches.
      # now its time to ``preserve'' the outer tuples with no matches.
      # this is totally inefficient: we should fold the identification of non-matches
      # into the join algorithms.  Another day.
      # our trick: for each tuple of the outer, generate a singleton relation
      # and join with inner.  If result is empty, preserve tuple.
      @rels[0].each do |r|
        t = @origrels[0].clone_empty
        t.insert(r)
        j = BudJoin.new([t,@origrels[1]], @bud_instance, @origpreds)
        next if j.any?
        nulltup = @origrels[1].null_tuple
        yield [r, nulltup]
      end
    end
  end

  class BudReadOnly < BudScratch
    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with read-only collection on left"
    end
    def merge
      raise BudError, "Illegale use of <= with read-only collection on left"
    end
  end

  class BudFileReader < BudReadOnly
    def initialize(name, filename, delimiter, bud_instance)
      super(name, ['lineno'], ['text'], bud_instance)
      @filename = filename
      @storage = {}
      # NEEDS A TRY/RESCUE BLOCK
      @fd = File.open(@filename, "r")
      @linenum = 0
    end
    
    def each(&block)
      while (l = @fd.gets)
        t = tuple_accessors([@linenum, l.strip])
        @linenum += 1
        yield t
      end
    end
    
    def tick
      self
    end
  end

  # Persistent table implementation based on TokyoCabinet.
  class BudTcTable < BudCollection
    def initialize(name, keys, cols, bud_instance)
      tc_dir = bud_instance.options[:tc_dir]
      raise "TC support must be enabled via 'tc_dir'" unless tc_dir
      unless File.exists?(tc_dir)
        Dir.mkdir(tc_dir)
        puts "Created directory: #{tc_dir}" unless bud_instance.options[:quiet]
      end

      dirname = "#{tc_dir}/bud_#{bud_instance.port}"
      unless File.exists?(dirname)
        Dir.mkdir(dirname)
        puts "Created directory: #{dirname}" unless bud_instance.options[:quiet]
      end

      super(name, keys, cols, bud_instance)
      @to_delete = []

      @hdb = TokyoCabinet::HDB.new
      db_fname = "#{dirname}/#{name}.tch"
      flags = TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT
      if bud_instance.options[:tc_truncate] == true
        flags |= TokyoCabinet::HDB::OTRUNC
      end
      if !@hdb.open(db_fname, flags)
        raise BudError, "Failed to open TokyoCabinet DB '#{db_fname}': #{@hdb.errmsg}"
      end
      @hdb.tranbegin
    end

    def init_storage
      # XXX: we can't easily use the @storage infrastructure provided by
      # BudCollection; issue #33
      @storage = nil
    end

    def [](key)
      key_s = Marshal.dump(key)
      val_s = @hdb[key_s]
      if val_s
        return make_tuple(key, Marshal.load(val_s))
      else
        return @delta[key]
      end
    end

    def make_tuple(k_ary, v_ary)
      t = Array.new(k_ary.length + v_ary.length)
      keys.each_with_index do |k,i|
        t[schema.index(k)] = k_ary[i]
      end
      cols.each_with_index do |c,i|
        t[schema.index(c)] = v_ary[i]
      end
      tuple_accessors(t)
    end

    def each(&block)
      each_delta(&block)
      @hdb.each do |k,v|
        k_ary = Marshal.load(k)
        v_ary = Marshal.load(v)
        yield make_tuple(k_ary, v_ary)
      end
    end

    def flush
      @hdb.trancommit
    end

    def close
      @hdb.close
    end

    def merge_to_hdb(buf)
      buf.each do |key,tuple|
        merge_tuple(key, tuple)
      end
    end

    def merge_tuple(key, tuple)
      val = cols.map{|c| tuple[schema.index(c)]}
      key_s = Marshal.dump(key)
      val_s = Marshal.dump(val)
      if @hdb.putkeep(key_s, val_s) == false
        old_tuple = self[key]
        raise_pk_error(tuple, old_tuple)
      end
    end

    # move all deltas and new_deltas to TC
    def install_deltas
      merge_to_hdb(@delta)
      merge_to_hdb(@new_delta)
      @delta = {}
      @new_delta = {}
    end

    # move deltas to TC, and new_deltas to deltas
    def tick_deltas
      merge_to_hdb(@delta)
      @delta = @new_delta
      @new_delta = {}
    end

    superator "<-" do |o|
      o.each do |tuple|
        @to_delete << tuple
      end
    end

    def insert(tuple)
      key = keys.map{|k| tuple[schema.index(k)]}
      merge_tuple(key, tuple)
    end

    alias << insert

    # Remove to_delete and then add pending to HDB
    def tick
      @to_delete.each do |tuple|
        k = keys.map{|c| tuple[schema.index(c)]}
        k_str = Marshal.dump(k)
        cols_str = @hdb[k_str]
        unless cols_str.nil?
          hdb_cols = Marshal.load(cols_str)
          delete_cols = cols.map{|c| tuple[schema.index(c)]}
          if hdb_cols == delete_cols
            @hdb.delete k_str
          end
        end
      end
      @to_delete = []

      merge_to_hdb(@pending)
      @pending = {}

      @hdb.trancommit
      @hdb.tranbegin
    end

    def method_missing(sym, *args, &block)
      @hdb.send sym, *args, &block
    end
  end

  # Persistent table implementation based on Zookeeper.
  class BudZkTable < BudCollection
    def initialize(name, zk_path, zk_addr, bud_instance)
      super(name, ["key"], ["value"], bud_instance)

      zk_path = zk_path.chomp("/") unless zk_path == "/"
      @zk = Zookeeper.new(zk_addr)
      @zk_path = zk_path
      @next_storage = {}
      @cb = Zookeeper::WatcherCallback.new {
        puts "Got callback!"
        get_and_watch
      }
      get_and_watch
    end

    def clone_empty
      raise BudError
    end

    def get_and_watch
      puts "hello, world"
      r = @zk.get_children(:path => @zk_path, :watcher => @cb)
      return unless r[:stat].exists

      # XXX: can we easily get snapshot isolation?
      new_children = {}
      r[:children].each do |c|
        child_path = @zk_path
        child_path += "/" unless child_path.end_with? "/"
        child_path += c

        get_r = @zk.get(:path => child_path)
        unless get_r[:stat].exists
          puts "Failed to fetch child: #{child_path}"
          return
        end
        data = get_r[:data] or ""
        new_children[c] = data
      end

      # We successfully fetched all the children of @zk_path; at the
      # next Bud tick, install the new data in @storage
      @next_storage = new_children
      puts "get_and_watch()!"
    end

    def tick
      @storage = @next_storage
      @new_storage = {}
      puts "tick()!"
    end

    def flush
      puts "flush()!"
    end

    def close
      puts "close()!"
    end

    superator "<~" do |o|
      raise BudError, "Not implemented yet"
    end

    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with zktable on left"
    end
  end
end

module Enumerable
  def rename(keys, cols=[])
    s = Bud::BudScratch.new('temp', keys, cols, nil)
    s.merge(self, s.storage)
    s
  end
end
