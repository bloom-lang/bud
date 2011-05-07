module Bud
  class BudJoin < BudCollection
    attr_accessor :rels, :origrels, :origpreds # :nodoc: all
    attr_reader :hash_tables # :nodoc: all

    def initialize(rellist, bud_instance, preds=[]) # :nodoc: all
      @schema = []
      @origpreds = preds
      @bud_instance = bud_instance
      @localpreds = nil

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

      setup_preds(preds) unless preds.empty?
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
      setup_preds(preds) unless preds.empty?
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
          next if (mode == :delta and left_rel == :storage and right_rel == :storage)
          if @localpreds.nil? or @localpreds.empty?
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
      setup_preds(preds) unless preds.empty?
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
    def lefts(*preds)
      unless preds.empty?
        @localpreds ||= []
        @localpreds += disambiguate_preds(preds)
      end
      map{ |l,r| l }
    end

    # given a * expression over 2 collections, form all combinations of items
    # that satisfy the predicates +preds+, and project only onto the attributes
    # of the second item
    public
    def rights(*preds)
      unless preds.empty?
        @localpreds ||= []
        @localpreds += disambiguate_preds(preds)
      end
      map{ |l,r| r }
    end

    # given a * expression over 2 collections, form all combos of items that
    # satisfy +preds+, and for any item from the 1st collection that has no
    # matches in the 2nd, nil-pad it and include it in the output.
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
      @localpreds = allpreds.reject { |p| p[0][0] != @rels[0].tabname }
      otherpreds = allpreds - @localpreds
      unless otherpreds.empty?
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
        p[1][0] == rel_list[0].tabname ? p.reverse : p
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

    private
    def nestloop_join(left_rel, right_rel, &block)
      @rels[0].each_from_sym([left_rel]) do |r|
        @rels[1].each_from_sym([right_rel]) do |s|
          s = [s] if origrels.length == 2
          yield([r] + s) if test_locals(r, s)
        end
      end
    end

    private
    # calculate the attribute position for the left table in the join ("left_offset")
    # the right table may itself be a nested tuple from a join, so calculate
    # the tuple offset ("right_subtuple") and the attribute position within it
    # ("right_offset")
    def join_offsets(pred)
      right_entry = pred[1]
      right_name, right_offset = right_entry[0], right_entry[1]
      left_entry = pred[0]
      left_name, left_offset = left_entry[0], left_entry[1]

      # determine which subtuple of right collection contains the table
      # referenced in RHS of pred.  note that right collection doesn't contain the
      # first entry in rels, which is the left collection
      right_subtuple = 0
      origrels[1..origrels.length].each_with_index do |t,i|
        if t.tabname == pred[1][0]
          right_subtuple = i
          break
        end
      end

      return left_offset, right_subtuple, right_offset
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
      left_offset, right_subtuple, right_offset = join_offsets(@localpreds.first)

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

          # insert into the prober's hashtable only if symmetric ...
          if probe_sym == other_sym
            @hash_tables[probe_ix][probe_sym][attrval] ||= []
            @hash_tables[probe_ix][probe_sym][attrval] << r
          end

          # ...and probe the other hashtable
          next if @hash_tables[other_ix][other_sym][attrval].nil?
          @hash_tables[other_ix][other_sym][attrval].each do |s_tup|
            if probe_ix == 0
              left = r; right = s_tup
            else
              left = s_tup; right = r
            end
            retval = left + right
            yield retval if test_locals(left[0], right, @localpreds.first)
          end
        end
      end
    end
  end

  module BudOuterJoin
    public
    def each(&block) # :nodoc:all
      super(&block)
      # previous line finds all the matches.
      # now its time to ``preserve'' the outer tuples with no matches.
      # this is totally inefficient: we should fold the identification of non-matches
      # into the join algorithms.  Another day.
      # our trick: for each tuple of the outer, generate a singleton relation
      # and join with inner.  If result is empty, preserve tuple.
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

