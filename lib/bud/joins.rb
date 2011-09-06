module Bud
  class BudJoin < BudCollection
    attr_accessor :rels, :origrels, :origpreds # :nodoc: all
    attr_reader :hash_tables # :nodoc: all

    def initialize(rellist, bud_instance, preds=[]) # :nodoc: all
      @schema = []
      @origpreds = preds
      @bud_instance = bud_instance
      @localpreds = nil
      @hashpreds = nil
      @selfjoins = []

      # if any elements on rellist are BudJoins, suck up their contents
      tmprels = []
      rellist.each do |r|
        if r.class <= BudJoin
          tmprels += r.origrels
          preds += r.origpreds
        else
          tmprels << r
        end
      end
      rellist = tmprels
      @origrels = rellist

      # check for self-joins: we currently only handle 2 instances of the same table per rule
      counts = @origrels.reduce({}) do |memo, r|
        memo[r.tabname] ||= 0
        memo[r.tabname] += 1
        memo
      end
      counts.each do |name, cnt| 
        raise Bud::CompileError, "#{cnt} instances of #{name} in rule; only one self-join currently allowed per rule" if cnt > 2
        @selfjoins << name if cnt == 2
      end
      

      # recurse to form a tree of binary BudJoins
      @rels = [rellist[0]]
      @rels << (rellist.length == 2 ? rellist[1] : BudJoin.new(rellist[1..rellist.length-1], @bud_instance))
      # derive schema: one column for each table.
      # duplicated inputs get distinguishing numeral
      @schema = []
      index = 0
      retval = rellist.reduce({}) do |memo, r|
        index += 1
        memo[r.tabname.to_s] ||= 0
        newstr = r.tabname.to_s + ((memo[r.tabname.to_s] > 0) ? ("_" + memo[r.tabname.to_s].to_s) : "")
        @schema << newstr.to_sym
        memo[r.tabname.to_s] += 1
        memo
      end

      setup_preds(preds)
      setup_state
    end

    public
    def state_id # :nodoc: all
      Marshal.dump([@rels.map{|r| r.tabname}, @localpreds]).hash
    end

    # initialize the state for this join to be carried across iterations within a fixpoint
    private
    def setup_state
      sid = state_id
      @tabname = ("temp_join"+state_id.to_s).to_sym
      @bud_instance.joinstate[sid] ||= [{:storage => {}, :delta => {}}, {:storage => {}, :delta => {}}]
      @hash_tables = @bud_instance.joinstate[sid]
    end

    private_class_method
    def self.natural_preds(bud_instance, rels)
      preds = []
      rels.each do |r|
        rels.each do |s|
          matches = r.schema & s.schema
          matches.each do |c|
            preds << [bud_instance.send(r.tabname).send(c), bud_instance.send(s.tabname).send(c)] unless r.tabname.to_s >= s.tabname.to_s
          end
        end
      end
      preds.uniq
    end

    # flatten joined items into arrays, with attribute accessors inherited
    # from the input collections, disambiguated via suffix indexes as needed.
    # similar to <tt>SELECT * FROM ... WHERE...</tt> block in SQL.
    public
    def flatten(*preds)
      setup_preds(preds)
      flat_schema = @rels.map{|r| r.schema}.flatten(1)
      dupfree_schema = []
      # while loop here (inefficiently) ensures no collisions
      while dupfree_schema == [] or dupfree_schema.uniq.length < dupfree_schema.length
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
      retval = BudScratch.new('temp_flatten', bud_instance, dupfree_schema)
      retval.uniquify_tabname
      retval.merge(self.map{|r,s| r + s}, retval.storage)
    end

    undef do_insert

    public
    # map each (nested) item in the collection into a string, suitable for placement in stdio
    def inspected
      raise BudError, "join left unconverted to binary" if @rels.length > 2
      self.map{|r1, r2| ["\[ #{r1.inspect} #{r2.inspect} \]"]}
    end

    public
    def pro(&blk) # :nodoc: all
      pairs(&blk)
    end

    public
    def each(mode=:both, &block) # :nodoc: all
      mode = :storage if @bud_instance.stratum_first_iter
      if mode == :storage
        methods = [:storage]
      else
        methods = [:delta, :storage]
      end

      methods.each do |left_rel|
        methods.each do |right_rel|
          next if (mode == :both and left_rel == :storage and right_rel == :storage)
          if @hashpreds.nil? or @hashpreds.empty?
            nestloop_join(left_rel, right_rel, &block)
          else
            hash_join(left_rel, right_rel, &block)
          end
        end
      end
      tick_hash_deltas
    end

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
      setup_preds(preds)
      # given new preds, the state for the join will be different.  set it up again.
      setup_state if self.class <= Bud::BudJoin
      blk.nil? ? self : map(&blk)
    end

    alias combos pairs
    
    # the natural join: given a * expression over n collections, form all
    # combinations of items that have the same values in matching fields
    public
    def matches(&blk)
      preds = BudJoin::natural_preds(@bud_instance, @origrels)
      pairs(*preds, &blk)
    end

    # given a * expression over 2 collections, form all combinations of items
    # that satisfy the predicates +preds+, and project only onto the attributes
    # of the first collection
    public
    def lefts(*preds, &blk)
      setup_preds(preds)
      # given new preds, the state for the join will be different.  set it up again.
      setup_state if self.class <= Bud::BudJoin
      map{ |l,r| blk.nil? ? l : blk.call(l) }
    end

    # given a * expression over 2 collections, form all combinations of items
    # that satisfy the predicates +preds+, and project only onto the attributes
    # of the second item
    public
    def rights(*preds, &blk)
      setup_preds(preds)
      # given new preds, the state for the join will be different.  set it up again.
      setup_state if self.class <= Bud::BudJoin
      map{ |l,r| blk.nil? ? r : blk.call(r) }
    end

    # given a * expression over 2 collections, form all combos of items that
    # satisfy +preds+, and for any item from the 1st collection that has no
    # matches in the 2nd, nil-pad it and include it in the output.
    public
    def outer(*preds, &blk)
      @origpreds = preds
      setup_preds(preds)
      self.extend(Bud::BudOuterJoin)
      blk.nil? ? self : map(&blk)
    end
    
    # AntiJoin
    public
    def nopairs(*preds, &blk)
      @origpreds = preds
      setup_preds(preds)
      setup_state if self.class <= Bud::BudJoin
      if @bud_instance.stratum_first_iter
        @matches = map { |r, s| (blk.nil?) ? r : blk.call(r,s) }.compact
        @rels[0].map {|r| (@matches.include? r) ? nil : r}.compact
      else
        []
      end
    end

    # extract predicates on rellist[0] and recurse to right side with remainder
    protected
    def setup_preds(preds) # :nodoc: all
      return if preds.empty?
      allpreds = disambiguate_preds(preds)
      allpreds = canonicalize_localpreds(@rels, allpreds)
      # check for refs to collections that aren't being joined, Issue 191
      unless @rels[1].class <= Bud::BudJoin
        tabnames = @rels.map{ |r| r.tabname }
        allpreds.each do |p|
          unless tabnames.include? p[0][0] and tabnames.include? p[1][0]
            raise Bud::CompileError, "illegal predicate: collection #{} is not being joined"
          end
        end
      end
      @hashpreds = allpreds.reject {|p| p[0][0] != @rels[0].tabname}
      @localpreds = @hashpreds
      
      # only allow preds on the same table name if they're on a self-joined table
      @localpreds.each do |p| 
        if p[0][0] == p[1][0] and not @selfjoins.include? p[0][0]
          raise Bud::CompileError, "single-table predicate on #{p[0][0]} disallowed in joins" 
        end
      end
      
      @localpreds += allpreds.map do |p|
        p if p[0][0] == p[1][0] and (p[0][0] == @rels[0].tabname or p[0][0] == @rels[1].tabname)
      end.compact
      otherpreds = allpreds - @localpreds
      unless otherpreds.empty?
        unless @rels[1].class <= Bud::BudJoin
          raise Bud::CompileError, "join predicates don't match tables being joined: #{otherpreds.inspect}"
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
              [find_attr_match(k, @origrels[0]), find_attr_match(v, @origrels[1])]
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

    # find element in @origrels that contains this +aname+ method
    # if +rel+ is non-nil, only check that collection.
    # after found, return the result of invoking +aname+ from chosen collection
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
        # reverse if rhs is rel_list[0], *unless* it's a self-join!
        (p[1][0] == rel_list[0].tabname and p[1][0] != p[0][0]) ? p.reverse : p
      end
    end

    public
    def each_from_sym(buf_syms, &block) # :nodoc: all
      buf_syms.each do |s|
        each(s, &block)
      end
    end

    private
    # r is a tuple
    # s is an array (combo) of joined tuples
    def test_locals(r, s, *skips)
      retval = true
      if (@localpreds and skips and @localpreds.length > skips.length)
        # check remainder of the predicates
        @localpreds.each do |pred|
          # skip skips, and self-join preds
          next if (skips.include? pred or pred[0][0] == pred[1][0])
          vals = []
          (0..1).each do |i|
            if pred[i][0] == @rels[0].tabname
              vals[i] = r[pred[i][1] ]
            else
              ix, off = join_offset(pred[i])
              vals[i] = s[ix][off]
            end
          end
          if vals[0] != vals[1]
            retval = false
            break
          end
        end
      end
      return retval
    end

    private
    def nestloop_join(left_rel, right_rel, &block)
      @rels[0].each_from_sym([left_rel]) do |r|
        @rels[1].each_from_sym([right_rel]) do |s|
          s = [s] if origrels.length == 2
          if test_locals(r, s)
              yield([r] + s)
          end
        end
      end
    end

    private
    # calculate the position for a field in the result of a join:
    # the tuple offset ("subtuple") and the attribute position within it
    # ("offset")
    def join_offset(entry)
      name, offset = entry[0], entry[1]

      # determine which subtuple of the collection contains the table
      # referenced in entry.  
      subtuple = 0
      origrels[1..origrels.length].each_with_index do |t,i|
        if t.tabname == entry[0]
          subtuple = i
          break
        end
      end

      return subtuple, offset
    end

    def tick_hash_deltas
      # for hash_join, move old delta hashtables into storage hashtables
      return if @hash_tables.nil?
      (0..1).each do |i|
        @hash_tables[i][:storage].merge!(@hash_tables[i][:delta]) do |k,l,r|
          l+r
        end
        @hash_tables[i][:delta] = {}
      end
    end

    # semi-naive symmetric hash join on first predicate
    private
    def hash_join(left_sym, right_sym, &block)
      # we know that a hashpred has been canonicalized with @rels[0] in left offset
      left_offset = @hashpreds.first[0][1]
      right_subtuple, right_offset = join_offset(@hashpreds.first[1])

      syms = [left_sym, right_sym]

      syms.each_with_index do |probe_sym, probe_ix|
        other_ix = 1 - probe_ix # bit-flip
        other_sym = syms[other_ix]
        probe_offset = (probe_ix == 0) ? left_offset : right_offset

        # in a delta/storage join we do traditional one-sided hash join
        # so don't probe from the storage side.
        # the other side should have been built already!
        if probe_sym == :storage and probe_sym != other_sym
          next
        end

        # ready to do the symmetric hash join
        rels[probe_ix].each_from_sym([probe_sym]) do |r|
          r = [r] unless probe_ix == 1 and origrels.length > 2
          attrval = (probe_ix == 0) ? r[0][left_offset] : r[right_subtuple][right_offset]

          # insert into the prober's hashtable only if symmetric
          if probe_sym == other_sym
            @hash_tables[probe_ix][probe_sym][attrval] ||= []
            @hash_tables[probe_ix][probe_sym][attrval] << r
          end

          # ...and probe the other hashtable
          if @hash_tables[other_ix][other_sym][attrval].nil?
            next
          else
            @hash_tables[other_ix][other_sym][attrval].each do |s_tup|
              if probe_ix == 0
                left = r; right = s_tup
              else
                left = s_tup; right = r
              end
              retval = left + right
              yield retval if test_locals(left[0], right, @hashpreds.first)
            end
          end
        end
      end
    end
  end

  # intended to be used to extend a BudJoin instance
  module BudOuterJoin
    public
    def each(&block) # :nodoc:all
      super(&block)
      # Previous line finds all the matches.  Now its time to ``preserve'' the
      # outer tuples with no matches.  Our trick: for each tuple of the outer,
      # generate a singleton relation and join with inner.  If result is empty,
      # preserve tuple.
      # XXX: This is totally inefficient: we should fold the identification of
      # non-matches into the join algorithms.  Another day.
      @rels[0].each do |r|
        t = @origrels[0].clone_empty
        # need to uniquify the tablename here to avoid sharing join state with original
        t.uniquify_tabname
        t << r
        j = BudJoin.new([t, @origrels[1]], @bud_instance, @origpreds)

        # the following is "next if j.any?" on storage tuples *only*
        any = false
        j.each(:storage) do |j|
          any = true
          break
        end
        next if any

        nulltup = @origrels[1].null_tuple
        yield [r, nulltup]
      end
    end
  end
end

