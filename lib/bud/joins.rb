module Bud
  class BudJoin < BudCollection
    attr_accessor :rels, :origrels, :origpreds

    def initialize(rellist, bud_instance, preds=nil)
      @schema = []
      otherpreds = nil
			@origpreds = preds
      @bud_instance = bud_instance
      @localpreds = nil
			@tabname = :temp_join
			
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
			self
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

		public
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
    
		public
    def each_from_sym(buf_syms, &block)
      buf_syms.each do |s|
        each(s, &block)
      end
    end

    # def each_storage(&block)
    #   each(:storage, &block)
    # end
    # 
    # # this needs to be made more efficient!
    # def each_delta(&block)
    #   each(:delta, &block)
    # end
		private
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
    def nestloop_join(collection1, collection2, &block)
      @rels[0].each_from_sym([collection1]) do |r|
        @rels[1].each_from_sym([collection2]) do |s|
          s = [s] if origrels.length == 2
          yield([r] + s) if test_locals(r, s)
        end
      end
    end

		private
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

		private
    def hash_join(collection1, collection2, &block)
      # hash join on first predicate!
      ht = {}

      probe_offset, build_tup, build_offset = join_offsets(@localpreds.first)

      # build the hashtable on s!
      rels[1].each_from_sym([collection2]) do |s|
        s = [s] if origrels.length == 2
        attrval = s[build_tup][build_offset]
        ht[attrval] ||= []
        ht[attrval] << s
      end

      # probe the hashtable!
      rels[0].each_from_sym([collection1]) do |r|
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
        t.insert(r)
        j = BudJoin.new([t,@origrels[1]], @bud_instance, @origpreds)
        next if j.any?
        nulltup = @origrels[1].null_tuple
        yield [r, nulltup]
      end
    end
  end
end