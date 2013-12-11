require 'bud/executor/elements'

module Bud
  class PushSHJoin < PushStatefulElement
    attr_reader :all_rels_below, :origpreds, :relnames, :keys, :localpreds

    def initialize(rellist, bud_instance, preds=nil) # :nodoc: all
      @rels = rellist
      @relnames = @rels.map{|r| r.qualified_tabname}
      @cols = []
      @bud_instance = bud_instance
      @origpreds = preds
      @localpreds = []
      @selfjoins = []
      @keys = []
      @key_attnos = [[], []]
      @missing_keys = Set.new

      # if any elements on rellist are PushSHJoins, suck up their contents
      @all_rels_below = []
      @rels.each do |r|
        if r.class <= PushSHJoin
          @all_rels_below += r.all_rels_below
          preds += r.origpreds
        else
          @all_rels_below << r
        end
      end
      @left_is_array = @all_rels_below.length > 2

      # check for self-joins: we currently only handle 2 instances of the same
      # table per rule
      counts = @all_rels_below.reduce({}) do |memo, r|
        memo[r.qualified_tabname] ||= 0
        memo[r.qualified_tabname] += 1
        memo
      end
      counts.each do |name, cnt|
        raise Bud::CompileError, "#{cnt} instances of #{name} in rule; only one self-join currently allowed per rule" if cnt > 2
        @selfjoins << name if cnt == 2
      end

      setup_preds(preds) unless preds.empty?
      setup_state

      super(@tabname, @bud_instance, nil, @cols)
    end

    public
    def state_id # :nodoc: all
      object_id
    end

    def flush
      replay_join if @rescan
    end

    # initialize the state for this join to be carried across iterations within a fixpoint
    private
    def setup_state
      sid = state_id
      @tabname = ("(" + @all_rels_below.map{|r| r.qualified_tabname}.join('*') +"):"+sid.to_s).to_sym
      @hash_tables = [{}, {}]
    end

    # extract predicates on rellist[1] and recurse to left side with remainder
    protected
    def setup_preds(preds) # :nodoc: all
      # print "setting up preds for #{@relnames.inspect}(#{self.object_id}): "
      allpreds = disambiguate_preds(preds)
      allpreds = canonicalize_localpreds(@rels, allpreds)

      # check for refs to collections that aren't being joined
      unless @rels[0].class <= Bud::PushSHJoin
        allpreds.each do |p|
          unless @relnames.include? p[0][0]
            raise Bud::CompileError, "illegal predicate: collection #{p[0][0]} is not being joined"
          end
          unless @relnames.include? p[1][0]
            raise Bud::CompileError, "illegal predicate: collection #{p[1][0]} is not being joined"
          end
        end
      end

      @localpreds = allpreds.reject do |p|
        # reject if it doesn't match the right (leaf node) of the join
        # or reject if it does match, but it can be evaluated by a lower join
        # i.e. one that also has this table on the right (leaf node)
        p[1][0] != @rels[1].qualified_tabname \
        or (p[0][0] != @rels[1].qualified_tabname \
            and p[1][0] == @rels[1].qualified_tabname and @selfjoins.include? @rels[1].qualified_tabname)
      end

      # only allow preds on the same table name if they're on a self-joined table
      @localpreds.each do |p|
        if p[0][0] == p[1][0] and not @selfjoins.include? p[0][0]
          raise Bud::CompileError, "single-table predicate on #{p[0][0]} disallowed in joins"
        end
      end

      @localpreds += allpreds.select do |p|
        p[0][0] == p[1][0] and (p[1][0] == @rels[0].qualified_tabname or p[1][0] == @rels[1].qualified_tabname)
      end
      otherpreds = allpreds - @localpreds
      unless otherpreds.empty?
        unless @rels[0].class <= Bud::PushSHJoin
          raise Bud::CompileError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
        end
        @rels[0].setup_preds(otherpreds)
      end

      @localpreds.each do |lp|
        right_offset = lp[1][1]
        left_subtuple, left_offset = join_offset(lp[0])
        @keys << [[left_subtuple, left_offset], [1, right_offset]]
      end

      # Optimize for a common case. When we're just fetching key values from
      # an input tuple, lookup the column offsets we need to fetch for each
      # input. This doesn't apply when we're computing the key for the left
      # input and @left_is_array is true.
      @key_attnos = []
      @key_attnos[0] = @keys.map {|k| k[0][1]}
      @key_attnos[1] = @keys.map {|k| k[1][1]}
    end

    public
    def invalidate_cache
      @rels.each_with_index do |source_elem, i|
        if source_elem.rescan
          puts "#{qualified_tabname} rel:#{i}(#{source_elem.qualified_tabname}) invalidated" if $BUD_DEBUG
          @hash_tables[i] = {}
        end
      end
    end

    # calculate the position for a field in the result of a join:
    # the tuple offset ("subtuple") and the attribute position within it
    # ("offset")
    def join_offset(entry)
      name, offset = entry[0], entry[1]

      # determine which subtuple of the collection contains the table
      # referenced in entry.
      subtuple = 0
      all_rels_below[0..all_rels_below.length-1].each_with_index do |t,i|
        if t.qualified_tabname == name
          subtuple = i
          break
        end
      end

      return subtuple, offset
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
            if @all_rels_below.length == 2
              [find_attr_match(k, @all_rels_below[0]), find_attr_match(v, @all_rels_below[1])]
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

    # find element in @all_rels_below that contains this +aname+ method
    # if +rel+ is non-nil, only check that collection.
    # after found, return the result of invoking +aname+ from chosen collection
    protected
    def find_attr_match(aname, rel=nil) # :nodoc: all
      dorels = (rel.nil? ? @all_rels_below : [rel])
      match = nil
      dorels.each do |r|
        r_name = r.qualified_tabname
        tbl = bud_instance.toplevel.tables[r_name]
        match ||= r if tbl.respond_to?(aname)
        if tbl.respond_to?(aname) and match != r
          raise Bud::CompileError, "ambiguous attribute :#{aname} in both #{match.qualified_tabname} and #{r_name}"
        end
      end
      if match.nil?
        rel_names = dorels.map{|t| t.qualified_tabname.to_s}.to_s
        raise Bud::CompileError, "attribute :#{aname} not found in any of #{rel_names}"
      end
      match.send(aname)
    end

    # decompose each pred into a binary pred
    protected
    def decomp_preds(*preds) # :nodoc:all
      return nil if preds.empty? or preds == [nil]
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
      second_rel = rel_list[1].qualified_tabname
      preds.map do |p|
        # reverse if lhs is second_rel *unless* it's a self-join!
        (p[0][0] == second_rel and p[0][0] != p[1][0]) ? p.reverse : p
      end
    end

    undef do_insert

    public
    def insert(item, source)
      # If we need to reproduce the join's output, do that now before we process
      # the to-be-inserted tuple. This avoids needless duplicates: if the
      # to-be-inserted tuple produced any join output, we'd produce that output
      # again if we didn't rescan now.
      replay_join if @rescan

      source_tbl = source.qualified_tabname
      if @selfjoins.include? source_tbl
        offsets = []
        @relnames.each_with_index{|r,i| offsets << i if r == source_tbl}
      else
        offsets = [@relnames.index(source_tbl)]
      end

      offsets.each {|offset| insert_item(item, offset)}
    end

    protected
    def insert_item(item, offset)
      # assumes left-deep trees
      if @left_is_array and offset == 0
        the_key = @keys.map do |k|
          left_subtuple, left_offset = k.first
          item[left_subtuple][left_offset]
        end
      else
        the_key = item.values_at(*@key_attnos[offset])
      end

      #build
      # puts "building #{item.inspect} into @source[#{offset}] on key #{the_key.inspect}"
      if (@hash_tables[offset][the_key] ||= Set.new).add? item
        @found_delta = true
        #and probe
        # puts "probing #{item.inspect} into @source[#{1-offset}] on key #{the_key.inspect}"
        the_matches = @hash_tables[1-offset][the_key]
        process_matches(item, the_matches, offset) unless the_matches.nil?
      end
    end

    def replay_join
      @rescan = false
      a, b = @hash_tables
      return if a.empty? or b.empty?

      if a.size < b.size
        a.each_pair do |key, items|
          the_matches = b[key]
          unless the_matches.nil?
            items.each do |item|
              process_matches(item, the_matches, 0)
            end
          end
        end
      else
        b.each_pair do |key, items|
          the_matches = a[key]
          unless the_matches.nil?
            items.each do |item|
              process_matches(item, the_matches, 1)
            end
          end
        end
      end
    end

    private
    def process_matches(item, the_matches, offset)
      the_matches.each do |m|
        if offset == 0
          left = item
          right = m
        else
          left = m
          right = item
        end

        # FIX: reduce arrays being created
        result = @left_is_array ? left + [right] : [left, right]
        push_out(result)
      end
    end

    ####
    # and now, the Bloom-facing methods
    # given a * expression over n collections, form all combinations of items
    # subject to an array of predicates, +preds+.
    # currently supports two syntax options for equijoin predicates:
    #    general form: an array of arrays capturing a conjunction of equiv. classes
    #          [[table1.col1, table2.col2, table3.col3], [table1.col2, table2.col3]]
    #    common form: a hash capturing equality of a column on left with one on right.
    #          :col1 => :col2  (same as  lefttable.col1 => righttable.col2)
    public
    def pairs(*preds, &blk)
      if @cols.nil?
        # derive schema if needed: one column for each table.  duplicated inputs
        # get distinguishing numeral.
        #
        # XXX: actually, this seems completely bogus. The schema for the output
        # of the join should depend on the join's *targetlist*.
        @cols = []
        retval = @all_rels_below.reduce({}) do |memo, r|
          r_name = r.qualified_tabname.to_s
          memo[r_name] ||= 0
          newstr = r_name + (memo[r_name] > 0 ? "_#{memo[r_name]}" : "")
          @cols << newstr.to_sym
          memo[r_name] += 1
          memo
        end
        setup_accessors
      end

      @origpreds = preds
      setup_preds(preds) unless preds.empty?
      # given new preds, the state for the join will be different.  set it up again.
      setup_state if self.class <= Bud::PushSHJoin
      set_block(&blk) if blk
      self
    end

    # given a * expression over 2 collections, form all combos of items that
    # satisfy +preds+, and for any item from the 1st collection that has no
    # matches in the 2nd, nil-pad it and include it in the output.
    public
    def outer(*preds, &blk)
      if @all_rels_below.length > 2
        raise Bud::Error, "outer joins cannot be used with more than 2 join relations"
      end
      pairs(*preds, &blk)
      self.extend(Bud::PushSHOuterJoin)
    end

    public
    def lefts(*preds, &blk)
      if blk.nil?
        @cols = @bud_instance.toplevel.tables[@rels[0].qualified_tabname].cols
        setup_accessors
      end
      pairs(*preds) do |x,y|
        blk.nil? ? x : blk.call(x)
      end
    end

    public
    def rights(*preds, &blk)
      if blk.nil?
        @cols = @bud_instance.toplevel.tables[@rels[1].qualified_tabname].cols
        setup_accessors
      end
      pairs(*preds) do |x,y|
        blk.nil? ? y : blk.call(y)
      end
    end

    private
    def dupfree_schema(flat_schema)
      dupfree_schema = []
      # while loop here (inefficiently) ensures no collisions
      while dupfree_schema.empty? or dupfree_schema.uniq.length < dupfree_schema.length
        dupfree_schema = []
        flat_schema.reduce({}) do |memo, r|
          if r.to_s.include?("_") and ((r.to_s.rpartition("_")[2] =~ /^\d+$/) == 0)
            r = r.to_s.rpartition("_")[0].to_sym
          end
          memo[r] ||= 0
          if memo[r] == 0
            dupfree_schema << r.to_s.to_sym
          else
            dupfree_schema << (r.to_s + "_" + (memo[r]).to_s).to_sym
          end
          memo[r] += 1
          memo
        end
        flat_schema = dupfree_schema
      end
      return flat_schema
    end

    public
    def flatten(*preds, &blk)
      if blk.nil?
        @cols = dupfree_schema(@rels[0].cols + @rels[1].cols)
      else
        @cols = []
      end
      setup_accessors
      pairs(*preds) do |x,y|
        blk.nil? ? x + y : blk.call(x + y)
      end
    end

    private_class_method
    def self.natural_preds(bud_instance, rels)
      preds = []
      rels.each_with_index do |r,i|
        rels.each_with_index do |s,j|
          unless i >= j
            the_matches = r.cols & s.cols
            the_matches.each do |c|
              preds << [r.send(c), s.send(c)]
            end
          end
        end
      end
      preds.uniq
    end

    public
    def matches(&blk)
      preds = self.class.natural_preds(@bud_instance, @all_rels_below)
      pairs(*preds, &blk)
    end

    alias combos pairs
  end

  module PushSHOuterJoin
    # XXX: duplicates code from PushSHJoin
    private
    def insert_item(item, offset)
      the_key = item.values_at(*@key_attnos[offset])

      #build
      # puts "building #{item.inspect} into @source[#{offset}] on key #{the_key.inspect}"
      if (@hash_tables[offset][the_key] ||= Set.new).add? item
        @found_delta = true
        #and probe
        # puts "probing #{item.inspect} into @source[#{1-offset}] on key #{the_key.inspect}"
        the_matches = @hash_tables[1-offset][the_key]
        if the_matches.nil? and offset == 0 # only doing Left Outer Join right now
          @missing_keys << the_key
        else
          # no longer missing no matter which side this tuple is
          @missing_keys.delete(the_key)
          process_matches(item, the_matches, offset) unless the_matches.nil?
        end
      end
    end

    public
    def rescan_at_tick
      true
    end

    public
    def stratum_end
      flush
      push_missing
    end

    private
    def push_missing
      left_hash = @hash_tables[0]
      null_tuple = @rels[1].null_tuple
      @missing_keys.each do |key|
        left_hash[key].each do |t|
          push_out([t, null_tuple])
        end
      end
    end

    public
    def invalidate_cache
      super
      # Only if need to check left join rel because outer joins in Bloom are
      # left outer joins.
      @missing_keys.clear if @rels.first.rescan
    end
  end


  # Consider "u <= s.notin(t, s.a => t.b)". notin is a non-monotonic operator,
  # where u depends positively on s, but negatively on t. Stratification ensures
  # that t is fully computed in a lower stratum, which means that we can expect
  # multiple iterators on s's side only. If t's scanner were to push its
  # elements down first, every insert of s merely needs to be cross checked with
  # the cached elements of 't', and pushed down to the next element if s notin
  # t. However, if s's scanner were to fire first, we have to wait until the
  # first flush, at which point we are sure to have seen all the t-side tuples
  # in this tick.
  class PushNotIn < PushStatefulElement
    def initialize(rellist, bud_instance, preds, &blk) # :nodoc: all
      @lhs, @rhs = rellist
      @lhs_keycols = nil
      @rhs_keycols = nil
      name_in = "#{@lhs.qualified_tabname}_notin_#{@rhs.qualified_tabname}".to_sym
      super(name_in, bud_instance, nil, @lhs.schema)
      setup_preds(preds) unless preds.empty?
      @rhs_rcvd = false
      @hash_tables = [{},{}]
      if @lhs_keycols.nil? and blk.nil?
        # Pointwise comparison. Could use zip, but it creates an array for each
        # field pair.
        blk = lambda {|lhs, rhs|
          lhs.to_a == rhs.to_a
        }
      end
      set_block(&blk)
    end

    def setup_preds(preds)
      # This is simpler than PushSHJoin's setup_preds, because notin is a binary
      # operator where both lhs and rhs are collections. preds is an array of
      # hash_pairs. For now assume that the attributes are in the same order as
      # the tables.
      @lhs_keycols, @rhs_keycols = preds.reduce([[], []]) do |memo, item|
        # each item is a hash
        item.each_pair do |l, r|
          memo[0] << find_col(l, @lhs)
          memo[1] << find_col(r, @rhs)
        end
        memo
      end
    end

    def find_col(colspec, rel)
      case colspec
      when Symbol
        unless rel.respond_to? colspec
          raise Bud::Error, "attribute :#{colspec} not found in #{rel.qualified_tabname}"
        end
        col_desc = rel.send(colspec)
      when Array
        col_desc = colspec
      else
        raise Bud::Error, "symbol or column spec expected. Got #{colspec}"
      end
      col_desc[1] # col_desc is of the form [tabname, colnum, colname, seqno]
    end

    def get_key(item, offset)
      keycols = (offset == 0 ? @lhs_keycols : @rhs_keycols)
      keycols.nil? ? [] : item.values_at(*keycols)
    end

    public
    def rescan_at_tick
      true
    end

    def insert(item, source)
      if source == @lhs && source == @rhs       # Self join
        do_insert(item, 0)
        do_insert(item, 1)
      else
        offset = source == @lhs ? 0 : 1
        do_insert(item, offset)
      end
    end

    def do_insert(item, offset)
      key = get_key(item, offset)
      (@hash_tables[offset][key] ||= Set.new).add item
      if @rhs_rcvd and offset == 0
        rhs_values = @hash_tables[1][key]
        process_match(item, rhs_values)
      end
    end

    def flush
      # When flush is called the first time, both lhs and rhs scanners have been
      # invoked, and because of stratification we know that the rhs is not
      # growing any more, until the next tick.
      unless @rhs_rcvd
        @rhs_rcvd = true
        rhs_hash = @hash_tables[1]
        @hash_tables[0].each do |key,values|
          rhs_values = rhs_hash[key]
          values.each {|item| process_match(item, rhs_values)}
        end
      end
    end

    def process_match(lhs_item, rhs_values)
      if rhs_values.nil?
        # no corresponding rhs. Include in output
        exclude = false
      elsif not @blk.nil?
        # for any lhs * rhs pair, if block returns true, do not push lhs. lhs is pushed
        # only if there is no match (anti-join)
        exclude = rhs_values.any?{|rhs_item| @blk.call(lhs_item, rhs_item)}
      else
        exclude = true
      end

      push_out(lhs_item, false) unless exclude
    end

    def invalidate_cache
      raise Bud::Error if @rhs_rcvd     # sanity check; should already be reset

      if @lhs.rescan
        puts "#{tabname} rel:#{@lhs.qualified_tabname} invalidated" if $BUD_DEBUG
        @hash_tables[0] = {}
      end
      if @rhs.rescan
        puts "#{tabname} rel:#{@rhs.qualified_tabname} invalidated" if $BUD_DEBUG
        @hash_tables[1] = {}
      end
    end

    def stratum_end
      @rhs_rcvd = false
    end
  end
end
