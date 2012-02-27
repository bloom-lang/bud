require "set"
require 'bud/collections'
ELEMENT_BUFSIZE = 1

module Bud
  # Usage example:
  # p = PushElement.new(:r) do |inp|
  #   puts "in block"
  #   [inp] if inp.class <= Numeric and inp%2 == 0
  # end

  # p.insert(1)
  # p.insert(nil)
  class PushElement < BudCollection
    attr_accessor :elem_name, :schema
    attr_accessor :rescan, :invalidated
    attr_reader :arity, :inputs, :found_delta, :refcount, :wired_by, :outputs
    
    def initialize(name_in, bud_instance, collection_name=nil, given_schema=nil, defer_schema=false, &blk)
      super(name_in, bud_instance, given_schema, defer_schema)
      @blk = blk
      @outputs = []
      @pendings = []
      @deletes = []
      @delete_keys = []
      @wired_by = []
      @elem_name = name_in
      @found_delta = false
      @refcount = 1
      @each_index = 0
      @collection_name = collection_name
      @invalidated = true
      @rescan = true
    end

    def wiring?
      @bud_instance.toplevel.done_wiring == false
    end

    def wirings
      @wirings ||= @outputs + @pendings + @deletes + @delete_keys
    end

    public
    def print_wiring(depth=0, accum = "")
      depth.times {print "  "}
      puts "#{accum} #{(self.object_id*2).to_s(16)}: #{elem_name} (#{self.class})"

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
            else
              puts "#{(o.object_id*2).to_s(16)}: (#{o.class.name})"
            end
          end
        end
      end
    end

    def check_wiring
      if @blk.nil? and @outputs.empty? and @pendings.empty? and @deletes.empty? and @delete_keys.empty?
        raise "no output specified for PushElement #{@elem_name}"
      end
    end
      
    def set_block(&blk)
      @blk = blk
    end
    def wire_to(element)
      unless element.methods.include? :insert or element.methods.include? "insert"
        raise BudError, "attempt to wire_to element without insert method" 
      end
      # elem_name = element.respond_to?(:tabname) ? element.tabname : element.elem_name
      # puts "wiring #{self.elem_name} to #{elem_name}"
      @outputs << element
      element.wired_by << self if element.respond_to? :wired_by
    end
    def wire_to_pending(element)
      raise BudError, "attempt to wire_to_pending element without pending_merge method" unless element.methods.include? "pending_merge" or element.methods.include? :pending_merge
      elem_name = element.respond_to?(:tabname) ? element.tabname : element.elem_name
      # puts "wiring #{self.elem_name} to #{elem_name}(pending)"
      @pendings << element
      element.wired_by << self if element.respond_to? :wired_by
    end
    def wire_to_delete(element)
      raise BudError, "attempt to wire_to_delete element without pending_delete method" unless element.methods.include? "pending_delete" or element.methods.include? :pending_delete
      elem_name = element.respond_to?(:tabname) ? element.tabname : element.elem_name
      # puts "wiring #{self.elem_name} to #{elem_name}(delete)"
      @deletes << element
      element.wired_by << self if element.respond_to? :wired_by
    end
    def wire_to_delete_by_key(element)
      raise BudError, "attempt to wire_to_delete_by_key element without pending_delete_keys method" unless element.methods.include? "pending_delete_keys" or element.methods.include? :pending_delete_keys
      elem_name = element.respond_to?(:tabname) ? element.tabname : element.elem_name
      # puts "wiring #{self.elem_name} to #{elem_name}(delete)"
      @delete_keys << element
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
      if item
        blk = @blk if do_block
        if blk
          item = item.to_a if blk.arity > 1
          begin
            item = blk.call item
          rescue Exception => e
            $stderr.puts "Error processing #{tabname} =================="
            raise e
          end
        end
        @outputs.each do |ou|
          if ou.class <= Bud::PushElement
            #the_name = ou.elem_name
            # puts "#{self.object_id%10000} (#{elem_name}) -> #{ou.object_id%10000} (#{the_name}): #{item.inspect}"
            ou.insert(item,self)
          elsif ou.class <= Bud::BudCollection
            # the_name = ou.tabname
            # puts "#{self.object_id%10000} (#{elem_name}) -> #{ou.object_id%10000} (#{the_name}): #{item.inspect}"
            ou.do_insert(item,ou.new_delta)
          else
            raise "Expected either a PushElement or a BudCollection"
          end
        end unless item.nil?
        # for all the following, o is a BudCollection
        @deletes.each{|o| o.pending_delete([item])} unless item.nil?
        @delete_keys.each{|o| o.pending_delete_keys([item])} unless item.nil?
        @pendings.each{|o| o.pending_merge([item])} unless item.nil?
      end
    end

    def non_temporal_predecessors
      @wired_by.map {|elem| elem if elem.outputs.include? self}
    end

    # default for stateless elements
    def add_rescan_invalidate(rescan, invalidate)
      # if any source element is rescanned, pass the mode to outputs by adding self. Also, add self to invalidated
      # to force other upstream elements (if any) to rescan.
      # if any target element is invalidated, mark self as invalidated, as a signal to upstream nodes, and then
      # set rescan on as a signal to all other downstream nodes to expect a full rescan.
      # In other words, both flags are set in either case.
      unless rescan.member? self
        if (@outputs.any? {|o| invalidate.member? o}) or non_temporal_predecessors.any?{|p| rescan.member? p}
          rescan << self
          invalidate << self
          @outputs.each {|o| o.add_rescan_invalidate(rescan, invalidate)  unless o.class <= PushElement}
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
      #override to get rid of cached information.
    end
    public
    def stratum_end
    end
    public
    def set_schema(schema)
      @schema=schema
      setup_accessors
    end

    
    ####
    # and now, the Bloom-facing methods
    public
    def pro(the_name = @elem_name, the_schema = @schema, &blk)
      toplevel = @bud_instance.toplevel
      elem = Bud::PushElement.new('project' + object_id.to_s, toplevel.this_rule_context, @collection_name, the_schema)
      #elem.init_schema(the_schema) unless the_schema.nil?
      self.wire_to(elem)
      elem.set_block(&blk)
      toplevel.push_elems[[self.object_id,:pro,blk]] = elem
      return elem
    end
    
    alias each pro
    
    public
    def each_with_index(the_name = elem_name, the_schema = schema, &blk)
      toplevel = @bud_instance.toplevel
      elem = Bud::PushEachWithIndex.new('each_with_index' + object_id.to_s, toplevel.this_rule_context, @collection_name)
      self.wire_to(elem)
      toplevel.push_elems[[self.object_id,:each,blk]] = elem
    end
    
    def join(elem2, &blk)
      # cached = @bud_instance.push_elems[[self.object_id,:join,[self,elem2], @bud_instance, blk]]
      # if cached.nil?
        elem2  = elem2.to_push_elem unless elem2.class <= PushElement
        toplevel = @bud_instance.toplevel
        join = Bud::PushSHJoin.new([self,elem2], toplevel.this_rule_context, [])
        self.wire_to(join)
        elem2.wire_to(join)
        toplevel.push_elems[[self.object_id,:join,[self,elem2], toplevel, blk]] = join
        toplevel.push_joins[toplevel.this_stratum] << join
      # else
      #   cached.refcount += 1
      # end
      return toplevel.push_elems[[self.object_id,:join,[self,elem2], toplevel, blk]]
    end
    def *(elem2, &blk)
      join(elem2, &blk)
    end
    def merge(source)
      if source.class <= PushElement and wiring?
        source.wire_to(self)
      else
        source.each{|i| self << i}
      end
    end
    alias <= merge
    superator "<~" do |o|
      raise BudError, "Illegal use of <~ with pusher '#{tabname}' on left"
    end

    superator "<-" do |o|
      raise BudError, "Illegal use of <- with pusher '#{tabname}' on left"
    end

    superator "<+" do |o|
      raise BudError, "Illegal use of <+ with pusher '#{tabname}' on left"
    end

    def group(keycols, *aggpairs, &blk)
      # establish schema
      keycols = [] if keycols.nil?
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
      
      aggpairs = aggpairs.map{|ap| ap[1].nil? ? [ap[0]] : [ap[0], canonicalize_col(ap[1])]}
      toplevel = @bud_instance.toplevel
      # if @bud_instance.push_elems[[self.object_id, :group, keycols, aggpairs, blk]].nil?
        g = Bud::PushGroup.new('grp'+Time.new.tv_usec.to_s, toplevel.this_rule_context, @collection_name, keycols, aggpairs, the_schema, &blk)
        self.wire_to(g)
        toplevel.push_elems[[self.object_id, :group, keycols, aggpairs, blk]] = g
      # end
      # toplevel.push_elems[[self.object_id, :group, keycols, aggpairs, blk]]
      return g
    end
    def argagg(aggname, gbkey_cols, collection, &blk)
      gbkey_cols = gbkey_cols.map{|c| canonicalize_col(c)}
      collection = canonicalize_col(collection)
      toplevel = @bud_instance.toplevel
      agg = toplevel.send(aggname, collection)[0]
      raise BudError, "#{aggname} not declared exemplary" unless agg.class <= Bud::ArgExemplary
      keynames = gbkey_cols.map do |k|
        if k.class == Symbol
          k.to_s
        else
          k[2]
        end
      end
      aggpairs = [[agg,collection]]
      # if toplevel.push_elems[[self.object_id,:argagg, gbkey_cols, aggpairs, blk]].nil?
        aa = Bud::PushArgAgg.new('argagg'+Time.new.tv_usec.to_s, toplevel.this_rule_context, @collection_name, gbkey_cols, aggpairs, @schema, &blk)
        self.wire_to(aa)
        toplevel.push_elems[[self.object_id,:argagg, gbkey_cols, aggpairs, blk]] = aa
      # end
      # return toplevel.push_elems[[self.object_id,:argagg, gbkey_cols, aggpairs, blk]]
      return aa
    end
    def argmax(gbcols, col, &blk)
      argagg(gbcols, Bud::max(col), blk)
    end
    def argmin(gbcols, col, &blk)
      argagg(gbcols, Bud::min(col), blk)
    end
    def sort(name=nil, bud_instance=nil, schema=nil, &blk)
      elem = Bud::PushSort.new(name, bud_instance, schema, &blk)
      wire_to(elem)
      elem
    end
    def push_predicate(pred_symbol, name=nil, bud_instance=nil, schema=nil, &blk)
      elem = Bud::PushPredicate.new(pred_symbol, name, bud_instance, schema, &blk)
      wire_to(elem)
      elem
    end
    def all?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:all?, name, bud_instance, schema, &blk)
    end
    def any?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:any?, name, bud_instance, schema, &blk)
    end
    def include?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:include?, name, bud_instance, schema, &blk)
    end
    def member?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:member?, name, bud_instance, schema, &blk)
    end
    def none?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:none?, name, bud_instance, schema, &blk)
    end
    def one?(name=nil, bud_instance=nil, schema=nil, &blk)
      push_predicate(:one?, name, bud_instance, schema, &blk)
    end
    
    def reduce(initial, &blk)
      @memo = initial
      retval = Bud::PushReduce.new('reduce'+Time.new.tv_usec.to_s, @bud_instance, @collection_name, @schema, initial, &blk)
      self.wire_to(retval)
      retval
    end
    
    alias on_exists? pro
    def on_include?(item, &blk)
      toplevel = @bud_instance.toplevel
      if toplevel.push_elems[[self.object_id,:on_include?, item, blk]].nil?
        inc = pro{|i| blk.call(item) if i == item and not blk.nil?}
        wire_to(inc)
        toplevel.push_elems[[self.object_id,:on_include?, item, blk]] = inc
      end
      toplevel.push_elems[[self.object_id,:on_include?, item, blk]]
    end
    def inspected
      toplevel = @bud_instance.toplevel
      if toplevel.push_elems[[self.object_id,:inspected]].nil?
        ins = pro{|i| [i.inspect]}
        self.wire_to(ins)
        toplevel.push_elems[[self.object_id,:inspected]] = ins
      end
      toplevel.push_elems[[self.object_id,:inspected]]
    end
    
    def to_enum
        # scr = @bud_instance.scratch(("scratch_" + Process.pid.to_s + "_" + object_id.to_s + "_" + rand(10000).to_s).to_sym, @schema)
        scr = []
        self.wire_to(scr)
        scr
    end
  end  

  class PushStatefulElement < PushElement
    def rescan_at_tick
      true
    end

    def add_rescan_invalidate(rescan, invalidate)
      # if an upstream node is set to rescan, a stateful node has no option but to invalidate its cache
      #
      # In addition, a stateful node always rescans its own contents.
      rescan << self

      unless invalidate.member? self
        if non_temporal_predecessors.any? {|p| rescan.member? p}
          invalidate << self
          # Note that at run-time (at tick)), one can look at the flags of source and target nodes to do partial
          # invalidation. See PushJoin.tick
        end
      end
      # Let the other non-element outputs know, since they are don't have wiring information
      @outputs.each {|o| o.add_rescan_invalidate(rescan, invalidate)  unless o.class <= PushElement}
    end
  end

  class PushPredicate < PushStatefulElement
    def initialize(pred_symbol, elem_name=nil, collection_name=nil, bud_instance=nil, schema_in=nil, &blk)
      @pred_symbol = pred_symbol
      @in_buf = []
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end
  
    def insert(item, source)
      @in_buf << item
    end

    public
    def stratum_end
      # always rescans
      @in_buf.send(@pred_symbol, @blk)
    end

    def invalidate_cache
      @in_buf.clear
    end
  end
  
  class PushSort < PushStatefulElement
    def initialize(elem_name=nil, bud_instance=nil, collection_name=nil, schema_in=nil, &blk)
      @sortbuf = []
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end
  
    def insert(item, source)
      @sortbuf << item
    end

    def flush
      unless @sortbuf.empty?
        @sortbuf.sort!(&@blk)
        @sortbuf.each do |t|
          push_out(t, false)
        end
        @sortbuf = []
      end
      nil
    end

    def invalidate_cache
      @sortbuf = []
    end
  end
  
  class ScannerElement < PushElement
    attr_reader :collection
    attr_reader :rescan_set, :invalidate_set
    def initialize(elem_name, bud_instance, collection_in, schema=collection_in.schema, &blk)
      # puts self.class
      super(elem_name, bud_instance, collection_in.tabname, schema)
      @collection = collection_in
      @rescan_set = []
      @invalidate_set = []
    end

    def rescan
      @rescan || @collection.invalidated
    end

    def rescan_at_tick
      @collection.invalidate_at_tick # need to scan afresh if collection invalidated.
    end

    def invalidate_at_tick(rescan, invalidate)
      # collection of others to rescan/invalidate if this scanner's collection were to be invalidated.
      @rescan_set = rescan
      @invalidate_set = invalidate
    end

    public
    def add_rescan_invalidate(rescan, invalidate)
      if invalidate.member? @collection or (@outputs.any? {|o| invalidate.member? o})
        rescan << self
      end
    end

    def scan(first_iter)
      if (first_iter)
        if @rescan
          # scan entire storage
          @collection.each_raw {|item|
            push_out(item)
          }
        else
          # In the first iteration, tick_delta would be non-null IFF the collection has grown in an earlier stratum
          @collection.tick_delta.each {|item| push_out(item)}
        end
      end

      # send deltas out in all cases
      @collection.delta.each_value {|item| push_out(item)}
    end
  end

  class PushReduce < PushStatefulElement
    def initialize(elem_name, bud_instance, collection_name, schema_in, initial, &blk)
      @memo = initial
      @blk = blk
      super(elem_name, bud_instance, collection_name, schema)
    end

    def insert(i, source=nil)
      @memo = @blk.call(@memo,i)
    end

    def invalidate_cache
      @memo.clear
    end

    public
    def stratum_end
       @memo.each do |k,v|
         push_out([k,v], false)
       end
     end
  end

  class PushEachWithIndex < PushStatefulElement
    def initialize(elem_name, bud_instance, collection_name)
      super(elem_name, bud_instance, collection_name)
      @each_index = 0
    end

    def add_rescan_invalidate(rescan, invalidate)
      # This is similar to PushStatefulElement, except that a PushEachWithIndex marks itself for both
      # rescan and invalidate when either a upstream or downstream node has been marked. This is because this
      # element does have some state (the index), but not the tuples to push downstream, so it has to request it
      # from upstream.
      rescan << self
      unless invalidate.member? self
        if non_temporal_predecessors.any? {|p| rescan.member? p}
          invalidate << self
        end
      end
      @outputs.each {|o| o.add_rescan_invalidate(rescan, invalidate)  unless o.class <= PushElement}
    end

    def invalidate_cache
      @each_index = 0
    end

    def stratum_end
      @each_index = 0
    end

    def insert(item, source=nil)
      ix = @each_index
      @each_index = ix + 1
      push_out([item, ix])
    end
  end
end
