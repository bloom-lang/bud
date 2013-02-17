require 'bud/executor/elements'

$EMPTY = []
module Bud
  class PushSHJoin < PushStatefulElement
    attr_reader :all_rels_below, :origpreds, :relnames, :keys, :localpreds

    def initialize(rellist, bud_instance, preds=nil) # :nodoc: all
      @rels = rellist
      @relnames = @rels.map{|r| r.elem_name}
      @cols = []
      @bud_instance = bud_instance
      @origpreds = preds
      @localpreds = nil
      @selfjoins = []
      @missing_keys = Set.new

      # if any elements on rellist are PushSHJoins, suck up their contents
      @all_rels_below = []
      rellist.each do |r|
        if r.class <= PushSHJoin
          @all_rels_below += r.all_rels_below
          preds += r.origpreds
        else
          @all_rels_below << r
        end
      end

      # check for self-joins: we currently only handle 2 instances of the same
      # table per rule
      counts = @all_rels_below.reduce({}) do |memo, r|
        memo[r.elem_name] ||= 0
        memo[r.elem_name] += 1
        memo
      end
      counts.each do |name, cnt|
        raise Bud::CompileError, "#{cnt} instances of #{name} in rule; only one self-join currently allowed per rule" if cnt > 2
        @selfjoins << name if cnt == 2
      end

      # derive schema: one column for each table.
      # duplicated inputs get distinguishing numeral
      @cols = []
      index = 0
      retval = @all_rels_below.reduce({}) do |memo, r|
        index += 1
        memo[r.tabname.to_s] ||= 0
        newstr = r.tabname.to_s + ((memo[r.tabname.to_s] > 0) ? ("_" + memo[r.tabname.to_s].to_s) : "")
        @cols << newstr.to_sym
        memo[r.tabname.to_s] += 1
        memo
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
      @tabname = ("(" + @all_rels_below.map{|r| r.tabname}.join('*') +"):"+sid.to_s).to_sym
      @hash_tables = [{}, {}]
    end

    # extract predicates on rellist[1] and recurse to left side with remainder
    protected
    def setup_preds(preds) # :nodoc: all
      # print "setting up preds for #{@relnames.inspect}(#{self.object_id}): "
      allpreds = disambiguate_preds(preds)
      allpreds = canonicalize_localpreds(@rels, allpreds)
      # check for refs to collections that aren't being joined, Issue 191
      unless @rels[0].class <= Bud::PushSHJoin
        tabnames = @rels.map{ |r| r.tabname }
        allpreds.each do |p|
          unless tabnames.include? p[0][0]
            raise Bud::CompileError, "illegal predicate: collection #{p[0][0]} is not being joined"
          end
          unless tabnames.include? p[1][0]
            raise Bud::CompileError, "illegal predicate: collection #{p[1][0]} is not being joined"
          end
        end
      end
      @localpreds = allpreds.reject do |p|
        # reject if it doesn't match the right (leaf node) of the join
        # or reject if it does match, but it can be evaluated by a lower join
        # i.e. one that also has this table on the right (lead node)
        p[1][0] != @rels[1].tabname \
        or (p[0][0] != @rels[1].tabname \
            and p[1][0] == @rels[1].tabname and @selfjoins.include? @rels[1].tabname)
      end

      # only allow preds on the same table name if they're on a self-joined table
      @localpreds.each do |p|
        if p[0][0] == p[1][0] and not @selfjoins.include? p[0][0]
          raise Bud::CompileError, "single-table predicate on #{p[0][0]} disallowed in joins"
        end
      end

      @localpreds += allpreds.map do |p|
        p if p[0][0] == p[1][0] and (p[1][0] == @rels[0].tabname or p[1][0] == @rels[1].tabname)
      end.compact
      otherpreds = allpreds - @localpreds
      unless otherpreds.empty?
        unless @rels[0].class <= Bud::PushSHJoin
          raise Bud::CompileError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
        end
        @rels[0].setup_preds(otherpreds)
      end

      if @localpreds.length > 0
        @right_offset = @localpreds.first[1][1]
        @left_subtuple, @left_offset = join_offset(@localpreds.first[0])
        @keys = [[@left_subtuple, @left_offset], [1, @right_offset]]
      else
        @keys = []
      end
    end

    public
    def invalidate_cache
      @rels.each_with_index do |source_elem, i|
        if source_elem.rescan
          puts "#{tabname} rel:#{i}(#{source_elem.tabname}) invalidated" if $BUD_DEBUG
          @hash_tables[i] = {}
          if i == 0
            # Only if i == 0 because outer joins in Bloom are left outer joins.
            # If i == 1, missing_keys will be corrected when items are populated
            # in the rhs fork.
            # XXX This is not modular. We are doing invalidation work for outer
            # joins, which is part of a separate module PushSHOuterJoin.
            @missing_keys.clear
          end
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
        if t.tabname == entry[0]
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
            if @all_rels_below and @all_rels_below.length == 2
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
        match ||= r if bud_instance.tables[r.elem_name].respond_to?(aname)
        if bud_instance.tables[r.elem_name].respond_to?(aname) and match != r
          raise Bud::CompileError, "ambiguous attribute :#{aname} in both #{match.tabname} and #{r.tabname}"
        end
      end
      if match.nil?
        raise Bud::CompileError, "attribute :#{aname} not found in any of #{dorels.map{|t| t.tabname}.inspect}"
      end
      bud_instance.tables[match.elem_name].send(aname)
    end

    protected
    def decomp_preds(*preds) # :nodoc:all
                             # decompose each pred into a binary pred
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
      retval = preds.map do |p|
        # reverse if lhs is rel_list[1], *unless* it's a self-join!
        (p[0][0] == rel_list[1].tabname and p[0][0] != p[1][0]) ? p.reverse : p
      end
    end

    private
    # right is a tuple
    # left is a tuple or an array (combo) of joined tuples.
    def test_locals(left, left_is_array, right, *skips)
      retval = true
      if (skips and @localpreds.length > skips.length)
        # check remainder of the predicates
        @localpreds.each do |pred|
          # skip skips
          next if (skips.include? pred)
          # assumption of left-deep joins here
          if pred[1][0] != @rels[1].tabname
            raise Bud::Error, "expected rhs table to be #{@rels[1].tabname}, not #{pred[1][0]}"
          end
          rfield = right[pred[1][1]]
          if left_is_array
            ix, off = join_offset(pred[0])
            lfield = left[ix][off]
          else
            lfield = left[pred[0][1]]
          end
          if lfield != rfield
            retval = false
            break
          end
        end
      end
      return retval
    end

    undef do_insert

    public
    def insert(item, source)
      # If we need to reproduce the join's output, do that now before we process
      # the to-be-inserted tuple. This avoids needless duplicates: if the
      # to-be-inserted tuple produced any join output, we'd produce that output
      # again if we didn't rescan now.
      replay_join if @rescan

      if @selfjoins.include? source.elem_name
        offsets = []
        @relnames.each_with_index{|r,i| offsets << i if r == source.elem_name}
      else
        offsets = [@relnames.index(source.elem_name)]
      end
      raise Bud::Error, "item #{item.inspect} inserted into join from unknown source #{source.elem_name}" if offsets == $EMPTY
      offsets.each do |offset|
        insert_item(item, offset)
      end
    end

    protected
    def insert_item(item, offset)
      if @keys.nil? or @keys.empty?
        the_key = nil
      else
        # assumes left-deep trees
        if all_rels_below.length > 2 and offset == 0
          the_key = item[@keys[0][0]][@keys[0][1]]
        else
          the_key = item[@keys[offset][1]]
        end
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
        left_is_array = all_rels_below.length > 2
        if @localpreds.nil? or @localpreds.length == 1 or test_locals(left, left_is_array, right, @localpreds.first)
          result = left_is_array ? left + [right] : [left, right] # FIX: reduce arrays being created.
          push_out(result)
        end
      end
    end

    ####
    # and now, the Bloom-facing methods
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
      pairs(*preds, &blk)
      self.extend(Bud::PushSHOuterJoin)
    end

    public
    def rights(*preds, &blk)
      @cols = blk.nil? ? @bud_instance.tables[@rels[1].tabname].cols : nil
      setup_accessors if blk.nil?
      pairs(*preds) do |x,y|
        blk.nil? ? y : blk.call(y)
      end
    end

    public
    def lefts(*preds, &blk)
      @cols = blk.nil? ? @bud_instance.tables[@rels[0].tabname].cols : nil
      setup_accessors if blk.nil?
      pairs(*preds) do |x,y|
        blk.nil? ? x : blk.call(x)
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
        @cols = dupfree_schema(@bud_instance.tables[@cols[0]].cols + @bud_instance.tables[@cols[1]].cols)
      else
        @cols = []
      end
      setup_accessors
      pairs(*preds) do |x,y|
        blk.nil? ? x.to_a + y.to_a : blk.call(x.to_a + y.to_a)
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
    private
    def insert_item(item, offset)
      if @keys.nil? or @keys.empty?
        the_key = nil
      else
        if all_rels_below.length > 2 and offset == 1
          the_key = item[@keys[1][0]][@keys[1][1]]
        else
          the_key = item[@keys[offset][1]]
        end
      end
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
          @missing_keys.delete(the_key) # no longer missing no matter which side this tuple is
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
      @missing_keys.each do |key|
        @hash_tables[0][key].each do |t|
          push_out([t, @rels[1].null_tuple])
        end
      end
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
    def initialize(rellist, bud_instance, preds=nil, &blk) # :nodoc: all
      @lhs, @rhs = rellist
      @lhs_keycols = nil
      @rhs_keycols = nil
      name_in = "#{@lhs.tabname}_notin_#{@rhs.tabname}"
      super(name_in, bud_instance)
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
      if colspec.is_a? Symbol
        col_desc = rel.send(colspec)
        raise Bud::Error, "unknown column #{colspec} in #{@rel.tabname}" if col_desc.nil?
      elsif colspec.is_a? Array
        col_desc = colspec
      else
        raise Bud::Error, "symbol or column spec expected. Got #{colspec}"
      end
      col_desc[1] # col_desc is of the form [tabname, colnum, colname]
    end

    def get_key(item, offset)
      keycols = offset == 0 ? @lhs_keycols : @rhs_keycols
      keycols.nil? ? $EMPTY : keycols.map{|col| item[col]}
    end

    public
    def rescan_at_tick
      true
    end

    def insert(item, source)
      offset = source == @lhs ? 0 : 1
      key = get_key(item, offset)
      (@hash_tables[offset][key] ||= Set.new).add item
      if @rhs_rcvd and offset == 0
        push_lhs(key, item)
      end
    end

    def flush
      # When flush is called the first time, both lhs and rhs scanners have been
      # invoked, and because of stratification we know that the rhs is not
      # growing any more, until the next tick.
      unless @rhs_rcvd
        @rhs_rcvd = true
        @hash_tables[0].each do |key,values|
          values.each {|item| push_lhs(key, item)}
        end
      end
    end

    def push_lhs(key, lhs_item)
      rhs_values = @hash_tables[1][key]
      process_match(lhs_item, rhs_values)
    end

    def process_match(lhs_item, rhs_values)
      exclude = true
      if rhs_values.nil?
        # no corresponding rhs. Include in output
        exclude = false
      elsif not @blk.nil?
        # for any lhs * rhs pair, if block returns true, do not push lhs. lhs is pushed
        # only if there is no match (anti-join)
        exclude = rhs_values.any?{|rhs_item| @blk.call(lhs_item, rhs_item)}
      end
      unless exclude
        push_out(lhs_item, false)
      end
    end

    def invalidate_cache
      if @lhs.rescan
        puts "#{tabname} rel:#{@lhs.tabname} invalidated" if $BUD_DEBUG
        @hash_tables[0] = {}
      end
      if @rhs.rescan
        puts "#{tabname} rel:#{@rhs.tabname} invalidated" if $BUD_DEBUG
        @hash_tables[1] = {}
        @rhs_rcvd = false
      end
    end

    def stratum_end
      @rhs_rcvd = false
    end
  end
end
