require 'bud/collections'

module Bud
  # Usage example:
  # p = PushElement.new(:r) do |inp|
  #   puts "in block"
  #   [inp] if inp.class <= Numeric and inp%2 == 0
  # end

  # p.insert(1)
  # p.insert(nil)
  class PushElement < BudCollection
    attr_accessor :rescan, :invalidated
    attr_accessor :elem_name
    attr_reader :found_delta, :wired_by, :outputs, :pendings

    def initialize(name_in, bud_instance, collection_name=nil, given_schema=nil, defer_schema=false, &blk)
      super(name_in, bud_instance, given_schema, defer_schema)
      @blk = blk
      @outputs = Set.new
      @pendings = Set.new
      @deletes = Set.new
      @delete_keys = Set.new
      @wired_by = []
      @elem_name = name_in
      @found_delta = false
      @collection_name = collection_name
      @invalidated = true
      @rescan = true
    end

    def wirings
      @wirings ||= @outputs + @pendings + @deletes + @delete_keys
    end

    public
    def print_wiring(depth=0, accum="")
      depth.times {print "  "}
      puts "#{accum} #{(self.object_id*2).to_s(16)}: #{qualified_tabname} (#{self.class})"

      [@outputs, @pendings, @deletes, @delete_keys].each do |kind|
        case kind.object_id
        when @outputs.object_id
          next_accum = "=> "
        when @pendings.object_id
          next_accum = "+> "
        when @deletes.object_id, @delete_keys.object_id
          next_accum = "-> "
        end

        kind.each do |o|
          if o.respond_to?(:print_wiring)
            o.print_wiring(depth+1, next_accum)
          else
            (depth+1).times {print "  "}
            print "#{next_accum} "
            if o.class <= Bud::BudCollection
              puts "#{(o.object_id*2).to_s(16)}: #{o.qualified_tabname} (#{o.class})"
            elsif o.class <= Bud::LatticeWrapper
              puts "#{o.inspect}"
            else
              puts "#{(o.object_id*2).to_s(16)}: (#{o.class.name})"
            end
          end
        end
      end
    end

    def check_wiring
      if @blk.nil? and @outputs.empty? and @pendings.empty? and @deletes.empty? and @delete_keys.empty?
        raise Bud::Error, "no output specified for PushElement #{@qualified_tabname}"
      end
    end

    def set_block(&blk)
      @blk = blk
    end

    def wire_to(element, kind=:output)
      unless @bud_instance.wiring?
        raise Bud::Error, "wire_to called outside wiring phase"
      end

      case kind
      when :output
        @outputs << element
      when :pending
        @pendings << element
      when :delete
        @deletes << element
      when :delete_by_key
        @delete_keys << element
      else
        raise Bud::Error, "unrecognized wiring kind: #{kind}"
      end

      element.wired_by << self if element.respond_to? :wired_by
    end

    def rescan_at_tick
      false
    end

    def insert(item, source=nil)
      push_out(item)
    end

    def tick
      invalidate_cache if @invalidated
    end

    def tick_deltas
      @found_delta = false
    end

    def push_out(item, do_block=true)
      if do_block && @blk
        item = item.to_a if @blk.arity > 1
        item = @blk.call item
        return if item.nil?
      end

      @outputs.each do |ou|
        if ou.class <= Bud::PushElement
          ou.insert(item, self)
        elsif ou.class <= Bud::BudCollection
          ou.do_insert(item, ou.new_delta)
        elsif ou.class <= Bud::LatticeWrapper
          ou.insert(item, self)
        else
          raise Bud::Error, "unexpected output target: #{ou.class}"
        end
      end

      # for the following, o is a BudCollection
      @deletes.each{|o| o.pending_delete([item])}
      @delete_keys.each{|o| o.pending_delete_keys([item])}

      # o is a LatticeWrapper or a BudCollection
      @pendings.each do |o|
        if o.class <= Bud::LatticeWrapper
          o <+ item
        else
          o.pending_merge([item])
        end
      end
    end

    # default for stateless elements
    public
    def add_rescan_invalidate(rescan, invalidate)
      # If any sources are in rescan mode, then put this node in rescan
      srcs = non_temporal_predecessors
      if srcs.any?{|p| rescan.member? p}
        rescan << self
      end

      # Pass the current state to each output collection and see if they end up
      # marking this node for rescan
      invalidate_tables(rescan, invalidate)

      # Finally, if this node is in rescan, pass the request on to all source
      # elements
      if rescan.member? self
        rescan.merge(srcs)
      end
    end

    def invalidate_tables(rescan, invalidate)
      # Exchange rescan and invalidate information with tables. If this node is
      # in rescan, it may invalidate an output table (if it is a scratch). And
      # if the output table is going to be invalidated, this node marks itself
      # for rescan to enable a refill of that table at run-time.
      [@outputs, @pendings].each do |v|
        v.each do |o|
          unless o.class <= PushElement
            o.add_rescan_invalidate(rescan, invalidate)
            rescan << self if invalidate.member? o
          end
        end
      end
    end

    def <<(i)
      insert(i, nil)
    end

    public
    def flush
    end
    def invalidate_cache
    end
    def stratum_end
    end

    ####
    # and now, the Bloom-facing methods
    # XXX: "the_name" parameter is unused
    public
    def pro(the_name=elem_name, the_schema=schema, &blk)
      toplevel = @bud_instance.toplevel
      elem = Bud::PushElement.new("project#{object_id}".to_sym,
                                  toplevel.this_rule_context,
                                  @collection_name, the_schema)
      self.wire_to(elem)
      elem.set_block(&blk)
      toplevel.push_elems[[self.object_id, :pro, blk]] = elem
      return elem
    end

    alias each pro

    public
    def each_with_index(&blk)
      toplevel = @bud_instance.toplevel
      elem = Bud::PushEachWithIndex.new("each_with_index#{object_id}".to_sym,
                                        toplevel.this_rule_context,
                                        @collection_name)
      elem.set_block(&blk)
      self.wire_to(elem)
      toplevel.push_elems[[self.object_id, :each_with_index, blk]] = elem
    end

    def join(elem2, &blk)
      elem2 = elem2.to_push_elem unless elem2.kind_of? PushElement
      toplevel = @bud_instance.toplevel
      join = Bud::PushSHJoin.new([self, elem2], toplevel.this_rule_context, [])
      self.wire_to(join)
      elem2.wire_to(join)
      toplevel.push_elems[[self.object_id, :join, [self, elem2], toplevel, blk]] = join
      toplevel.push_joins[toplevel.this_stratum] << join
      return join
    end
    def *(elem2, &blk)
      join(elem2, &blk)
    end

    def notin(elem2, *preds, &blk)
      elem2 = elem2.to_push_elem unless elem2.kind_of? PushElement
      toplevel = @bud_instance.toplevel
      notin_elem = Bud::PushNotIn.new([self, elem2], toplevel.this_rule_context, preds, &blk)
      self.wire_to(notin_elem)
      elem2.wire_to(notin_elem)
      toplevel.push_elems[[self.object_id, :notin, [self, elem2], toplevel, blk]] = notin_elem
      return notin_elem
    end

    def merge(source)
      if source.class <= PushElement and @bud_instance.wiring?
        source.wire_to(self)
      else
        source.each {|i| self << i}
      end
    end
    alias <= merge

    superator "<~" do |o|
      raise Bud::Error, "illegal use of <~ with pusher '#{tabname}' on left"
    end

    superator "<-" do |o|
      raise Bud::Error, "illegal use of <- with pusher '#{tabname}' on left"
    end

    superator "<+" do |o|
      raise Bud::Error, "illegal use of <+ with pusher '#{tabname}' on left"
    end

    def group(keycols, *aggpairs, &blk)
      # establish schema
      keycols ||= []
      keycols = keycols.map{|c| canonicalize_col(c)}
      keynames = keycols.map{|k| k[2]}
      aggcolsdups = aggpairs.map{|ap| ap[0].class.name.split("::").last}
      aggcols = []
      aggcolsdups.each_with_index do |n, i|
        aggcols << "#{n.downcase}_#{i}".to_sym
      end
      if aggcols.empty?
        the_schema = keynames
      else
        the_schema = { keynames => aggcols }
      end

      aggpairs = prep_aggpairs(aggpairs)
      toplevel = @bud_instance.toplevel
      g = Bud::PushGroup.new("grp#{Time.new.tv_usec}".to_sym, toplevel.this_rule_context,
                             @collection_name, keycols, aggpairs, the_schema, &blk)
      self.wire_to(g)
      toplevel.push_elems[[self.object_id, :group, keycols, aggpairs, blk]] = g
      return g
    end

    def argagg(aggname, gbkey_cols, collection, &blk)
      gbkey_cols ||= []
      gbkey_cols = gbkey_cols.map{|c| canonicalize_col(c)}
      collection = canonicalize_col(collection)
      toplevel = @bud_instance.toplevel
      agg = toplevel.send(aggname, collection)[0]
      unless agg.class <= Bud::ArgExemplary
        raise Bud::Error, "#{aggname} not declared exemplary"
      end

      aggpairs = [[agg, collection]]
      aa = Bud::PushArgAgg.new("argagg#{Time.new.tv_usec}".to_sym, toplevel.this_rule_context,
                               @collection_name, gbkey_cols, aggpairs, schema, &blk)
      self.wire_to(aa)
      toplevel.push_elems[[self.object_id, :argagg, gbkey_cols, aggpairs, blk]] = aa
      return aa
    end
    def argmax(gbcols, col, &blk)
      argagg(:max, gbcols, col, &blk)
    end
    def argmin(gbcols, col, &blk)
      argagg(:min, gbcols, col, &blk)
    end
    def sort(name=nil, bud_instance=nil, the_schema=nil, &blk)
      elem = Bud::PushSort.new(name, bud_instance, the_schema, &blk)
      wire_to(elem)
      elem
    end
    def push_predicate(pred_symbol, name=nil, bud_instance=nil,
                       the_schema=nil, &blk)
      elem = Bud::PushPredicate.new(pred_symbol, name, bud_instance,
                                    the_schema, &blk)
      wire_to(elem)
      elem
    end
    def all?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:all?, name, bud_instance, the_schema, &blk)
    end
    def any?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:any?, name, bud_instance, the_schema, &blk)
    end
    def include?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:include?, name, bud_instance, the_schema, &blk)
    end
    def member?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:member?, name, bud_instance, the_schema, &blk)
    end
    def none?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:none?, name, bud_instance, the_schema, &blk)
    end
    def one?(name=nil, bud_instance=nil, the_schema=nil, &blk)
      push_predicate(:one?, name, bud_instance, the_schema, &blk)
    end

    def reduce(initial, &blk)
      retval = Bud::PushReduce.new("reduce#{Time.new.tv_usec}".to_sym,
                                   @bud_instance, @collection_name,
                                   schema, initial, &blk)
      self.wire_to(retval)
      retval
    end

    alias on_exists? pro
    def on_include?(item, &blk)
      toplevel = @bud_instance.toplevel
      if toplevel.push_elems[[self.object_id, :on_include?, item, blk]].nil?
        inc = pro{|i| blk.call(item) if i == item and not blk.nil?}
        wire_to(inc)
        toplevel.push_elems[[self.object_id, :on_include?, item, blk]] = inc
      end
      toplevel.push_elems[[self.object_id, :on_include?, item, blk]]
    end
    def inspected
      toplevel = @bud_instance.toplevel
      if toplevel.push_elems[[self.object_id, :inspected]].nil?
        ins = pro{|i| [i.inspect]}
        self.wire_to(ins)
        toplevel.push_elems[[self.object_id, :inspected]] = ins
      end
      toplevel.push_elems[[self.object_id, :inspected]]
    end
  end

  class PushStatefulElement < PushElement
    def add_rescan_invalidate(rescan, invalidate)
      if non_temporal_predecessors.any? {|e| rescan.member? e}
        rescan << self
        invalidate << self
      end

      # Note that we do not need to pass rescan requests up to our source
      # elements, since a stateful element has enough local information to
      # reproduce its output.
      invalidate_tables(rescan, invalidate)
    end
  end

  class PushPredicate < PushStatefulElement
    def initialize(pred_symbol, elem_name=nil, collection_name=nil,
                   bud_instance=nil, schema_in=nil, &blk)
      @pred_symbol = pred_symbol
      @in_buf = []
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end

    def insert(item, source)
      @in_buf << item
    end

    public
    def flush
      # always rescans
      @in_buf.send(@pred_symbol, @blk)
    end

    def invalidate_cache
      @in_buf.clear
    end
  end

  class PushSort < PushStatefulElement
    def initialize(elem_name=nil, bud_instance=nil, collection_name=nil,
                   schema_in=nil, &blk)
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
      @sortbuf = []
      @seen_new_input = false
    end

    def insert(item, source)
      @sortbuf << item
      @seen_new_input = true
    end

    def flush
      if @seen_new_input || @rescan
        @sortbuf.sort!(&@blk)
        @sortbuf.each do |t|
          push_out(t, false)
        end
        @seen_new_input = false
        @rescan = false
      end
    end

    def invalidate_cache
      @sortbuf.clear
    end
  end

  class ScannerElement < PushElement
    attr_reader :collection
    attr_reader :rescan_set, :invalidate_set
    attr_accessor :force_rescan

    def initialize(elem_name, bud_instance, collection_in,
                   the_schema=collection_in.schema, &blk)
      super(elem_name, bud_instance, collection_in.qualified_tabname, the_schema)
      @collection = collection_in
      @rescan_set = []
      @invalidate_set = []
      @force_rescan = false
    end

    def rescan
      @rescan || @collection.invalidated
    end

    def rescan_at_tick
      @collection.invalidate_at_tick # need to scan afresh if collection invalidated.
    end

    # What should be rescanned/invalidated if this scanner's collection were to
    # be invalidated.
    def invalidate_at_tick(rescan, invalidate)
      @rescan_set = rescan
      @invalidate_set = invalidate
    end

    def add_rescan_invalidate(rescan, invalidate)
      # If the collection is to be invalidated, the scanner needs to be in
      # rescan mode
      rescan << self if invalidate.member? @collection

      # Pass the current state to each output collection and see if they end up
      # marking this node for rescan
      invalidate_tables(rescan, invalidate)

      # Note also that this node can be nominated for rescan by a target node;
      # in other words, a scanner element can be set to rescan even if the
      # collection is not invalidated.
    end

    def scan(first_iter)
      if @force_rescan
        @collection.each_raw {|item| push_out(item)}
        @force_rescan = false
      elsif first_iter
        if rescan
          @collection.each_raw {|item| push_out(item)}
        else
          # In the first iteration, tick_delta would be non-null IFF the
          # collection has grown in an earlier stratum
          @collection.each_tick_delta {|item| push_out(item)}
        end
      end

      # send deltas out in all cases
      @collection.each_delta {|item| push_out(item)}
    end
  end

  class PushReduce < PushStatefulElement
    def initialize(elem_name, bud_instance, collection_name,
                   schema_in, initial, &blk)
      @initial = initial
      @blk = blk
      reset_memo
      super(elem_name, bud_instance, collection_name, schema)
    end

    def insert(i, source=nil)
      @memo = @blk.call(@memo, i)
    end

    def invalidate_cache
      puts "#{self.class}/#{self.tabname} invalidated" if $BUD_DEBUG
      reset_memo
    end

    def reset_memo
      @memo = Marshal.load(Marshal.dump(@initial))
    end

    public
    def flush
      unless @memo.kind_of? Enumerable
        raise Bud::TypeError, "output of reduce must be Enumerable: #{@memo.inspect}"
      end
      @memo.each do |t|
        push_out(t, false)
      end
    end
  end

  class PushEachWithIndex < PushStatefulElement
    def initialize(elem_name, bud_instance, collection_name)
      super(elem_name, bud_instance, collection_name)
      @each_index = 0
    end

    def add_rescan_invalidate(rescan, invalidate)
      super

      # This node has some state (@each_index), but not the tuples. If it is in
      # rescan mode, then it must ask its sources to rescan, and restart its
      # index.
      if rescan.member? self
        invalidate << self
        srcs = non_temporal_predecessors
        rescan.merge(srcs)
      end
    end

    def invalidate_cache
      @each_index = 0
    end

    def stratum_end
      @each_index = 0
    end

    def insert(item, source=nil)
      ix = @each_index
      @each_index += 1
      push_out([item, ix])
    end
  end
end
