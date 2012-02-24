require 'bud/executor/elements'
require 'set'

$EMPTY = []
module Bud
  class PushSHJoin < PushStatefulElement
    attr_reader :all_rels_below, :origpreds, :relnames, :keys, :localpreds
    def initialize(rellist, bud_instance, preds=nil) # :nodoc: all
      @rels = rellist
      @relnames = @rels.map{|r| r.elem_name}
      @schema = []
      @bud_instance = bud_instance
      @origpreds = preds
      @localpreds = nil
      @selfjoins = []
      @input_bufs=[[],[]]
      the_join = nil

      # if any elements on rellist are PushSHJoins, suck up their contents
      @all_rels_below = []
      rellist.each do |r|
        if r.class <= PushSHJoin
          @all_rels_below += r.all_rels_below
          preds += r.origpreds
          the_join = r
        else
          @all_rels_below << r
        end
      end

      # check for self-joins: we currently only handle 2 instances of the same table per rule
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
      schema = []
      index = 0
      retval = @all_rels_below.reduce({}) do |memo, r|
        index += 1
        memo[r.tabname.to_s] ||= 0
        newstr = r.tabname.to_s + ((memo[r.tabname.to_s] > 0) ? ("_" + memo[r.tabname.to_s].to_s) : "")
        schema << newstr.to_sym
        memo[r.tabname.to_s] += 1
        memo
      end

      setup_preds(preds) unless preds.empty?
      setup_state

      super(@tabname,@bud_instance,nil,schema)
    end

    public
    def copy_on_write
      @refcount -= 1
      return Bud::PushSHJoin.new(@all_rels_below, @bud_instance, [])
    end

    public
    def state_id # :nodoc: all
      object_id
                 # Marshal.dump([@rels.map{|r| r.tabname}, @localpreds]).hash
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
                           # puts "@keys = #{@keys.inspect}"
    end

    public
    def invalidate_cache
      @rels.each_with_index do |source_elem, i|
        if source_elem.rescan
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
            raise "Expected rhs table to be #{@rels[1].tabname}, not #{pred[1][0]}"
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

    # given a * expression over 2 collections, form all combos of items that
    # satisfy +preds+, and for any item from the 1st collection that has no
    # matches in the 2nd, nil-pad it and include it in the output.
    public
    def join(elem2, &blk)
      elem2 = elem2.to_push_elem unless elem2.class <= PushElement
      # This constructs a left-deep tree!
      join = Bud::PushSHJoin.new([self,elem2], @bud_instance, [])
      @bud_instance.push_joins[@bud_instance.this_stratum] << join
      elem2.wire_to(join)
      self.wire_to(join)
      return join
    end

    undef do_insert

    public
    def insert(item, source)
      #puts "JOIN: #{source.tabname} -->  #{self.tabname} : #{item}/#{item.class}"
      if @rescan
        replay_join
        @rescan = false
      end
      if @selfjoins.include? source.elem_name
        offsets = []
        @relnames.each_with_index{|r,i| offsets << i if r == source.elem_name}
      else
        offsets = [@relnames.index(source.elem_name)]
      end
      raise "item #{item.inspect} inserted into join from unknown source #{source.elem_name}" if offsets == $EMPTY
      offsets.each do |offset|
        buf = @input_bufs[offset]
        buf << item
        if (buf.length >= ELEMENT_BUFSIZE)
          flush_buf(buf, offset)
        end
      end
    end

    private
    def insert_item(item, offset)
      if (@keys.nil? or @keys.empty?)
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
      a = @hash_tables[0]
      b = @hash_tables[1]

      if not(a.empty? or b.empty?)
        if a.size < b.size
          a.each_pair do |key, items|
            the_matches = b[the_key]
            unless the_matches.nil?
              items.each do |item|
                process_matches(item, the_matches, 1)
              end
            end
          end
        else
          b.each_pair do |key, items|
            the_matches = a[the_key]
            unless the_matches.nil?
              items.each do |item|
                process_matches(item, the_matches, 0)
              end
            end
          end
        end
      end

      @rescan = false
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
#        else
          # puts "    rejected #{result.inspect}"
        end
      end
    end

    def flush_buf(buf, offset)
      buf.each do |item|
        insert_item(item, offset)
      end
      @input_bufs[offset] = []
    end

    public
    def flush
      @input_bufs.each_with_index do |buf, offset|
        flush_buf(buf,offset) if buf.length > 0
      end
    end

    public
    def stratum_end
      flush
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
      ## XXX Need to do this for all the join modifiers
      unless @refcount == 1
        return self.copy_on_write.pairs(preds, blk)
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
      pairs(*preds, &blk)
      self.extend(Bud::PushSHOuterJoin)
    end


    public
    def rights(*preds, &blk)
      @schema = blk.nil? ? @bud_instance.tables[@rels[1].tabname].schema : nil
      setup_accessors if blk.nil?
      pairs(*preds) do |x,y|
        blk.nil? ? y : blk.call(y)
      end
    end

    public
    def lefts(*preds, &blk)
      @schema = blk.nil? ? @bud_instance.tables[@rels[0].tabname].schema : nil
      setup_accessors if blk.nil?
      pairs(*preds) do |x,y|
        blk.nil? ? x : blk.call(x)
      end
    end

    private
    def dupfree_schema(flat_schema)
      dupfree_schema = []
      # while loop here (inefficiently) ensures no collisions
      while dupfree_schema == $EMPTY or dupfree_schema.uniq.length < dupfree_schema.length
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
        @schema = dupfree_schema(@bud_instance.tables[@schema[0]].schema + @bud_instance.tables[@schema[1]].schema)
      else
        @schema = nil
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
            the_matches = r.schema & s.schema
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
      @missing_keys ||= Set.new
      if (@keys.nil? or @keys.empty?)
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
    def stratum_end
      flush
      push_missing
      #@hash_tables = [{},{}]
    end

    private
    def push_missing
      if @missing_keys
        @missing_keys.each do |key|
          @hash_tables[0][key].each do |t|
            push_out([t, []])
          end
        end
      end
    end
  end
end