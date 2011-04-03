module Bud
  class BudJoin < BudCollection
    attr_accessor :rels, :origrels, :origpreds
    attr_reader :hash_tables

    def initialize(rellist, bud_instance, preds=nil)
      @schema = []
      otherpreds = nil
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
      @rels << (rellist.length == 2 ? rellist[1] : BudJoin.new(rellist[1..rellist.length-1], @bud_instance, nil))
      # derive schema: one column for each table.
      # unnamed inputs become "t_i" for position i
      # duplicated inputs get distinguishing numeral
      @schema = []
      index = 0
      retval = rellist.reduce({}) do |memo, r|
        index += 1
        if !r.respond_to?(:tabname)
          @schema << "t_#{index}".to_sym
        else
          memo[r.tabname.to_s] ||= 0
					newstr = r.tabname.to_s + ((memo[r.tabname.to_s] > 0) ? ("_" + memo[r.tabname.to_s].to_s) : "")
          @schema << newstr.to_sym
          memo[r.tabname.to_s] += 1
        end
        memo
      end
      
      preds = setup_preds(preds) unless preds.nil? or preds.empty?
      
      setup_state
			self
    end

    public
    def state_id
      Marshal.dump([@rels.map{|r| r.tabname},@localpreds]).hash
    end

    # initialize the state for this join to be carried across iterations within a fixpoint
    private 
    def setup_state
      sid = state_id
      @tabname = ("temp_join"+state_id.to_s).to_sym
      @bud_instance.joinstate[sid] ||= [{:storage => {}, :delta => {}}, {:storage => {}, :delta => {}}]
      @hash_tables = @bud_instance.joinstate[sid]
      found = false
      (0..1).each do |i|
        found = true if @hash_tables[i][:storage].any? or @hash_tables[i][:delta].any?
      end
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
				
		public
    def flatten(*preds)
			unless preds.nil? or preds.size == 0
				@localpreds = disambiguate_preds(preds)
				canonicalize_localpreds(@rels)
			end
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
		
		private
    def do_insert(o, store)
      raise BudError, "no insertion into joins"
    end

		public
    def inspected
      if @rels.length == 2 then
        # fast common case
        self.map{|r1, r2| ["\[ #{r1.inspect} #{r2.inspect} \]"]}
      else
        str = "self.map\{|"
        (1..@rels.length-1).each{|i| str << "r#{i},"}
        str << "r#{@rels.length}| \[\"\[ "
        (1..@rels.length).each{|i| str << "\#\{r#{i}.inspect\} "}
        str << "\]\"\]\}"
        eval(str)
      end
    end

    # public 
    # def pro(&blk)
    #   map(&blk)
    # end

		public
    def each(mode=:both, &block)
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
    
		public
    def each_from_sym(buf_syms, &block)
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
            yield(retval) if test_locals(left[0], right, @localpreds.first)
          end
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

		public
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