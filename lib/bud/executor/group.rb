require 'bud/executor/elements'

module Bud
  class PushGroup < PushElement
    def initialize(elem_name, bud_instance, collection_name, keys_in, aggpairs_in, schema_in, &blk)
      @groups = {}
      if keys_in.nil?
        @keys = [] 
      else
        @keys = keys_in.map{|k| k[1]}
      end
      # ap[1] is nil for Count
      @aggpairs = aggpairs_in.map{|ap| ap[1].nil? ? [ap[0]] : [ap[0], ap[1][1]]}
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end
  
    def insert(item, source)
      key = @keys.map{|k| item[k]}   
      @aggpairs.each_with_index do |ap, agg_ix|
        agg_input = ap[1].nil? ? item : item[ap[1]]
        agg = (@groups[key].nil? or @groups[key][agg_ix].nil?) ? ap[0].send(:init, agg_input) : ap[0].send(:trans, @groups[key][agg_ix], agg_input)[0]
        @groups[key] ||= Array.new(@aggpairs.length)
        @groups[key][agg_ix] = agg
        push_out(nil)
      end
    end

    def tick
      @invalidated = @wired_by.any? {|w| w.invalidated}
      #invalidate_cache if @invalidated
    end

    def invalidate_cache
      @groups = {}
    end
    
    def local_flush#end(source)
      @groups.each do |g, grps|
        grp = @keys == $EMPTY ? [[]] : [g]
        @aggpairs.each_with_index do |ap, agg_ix|
          grp << ap[0].send(:final, grps[agg_ix])
        end
        outval = grp[0].flatten
        (1..grp.length-1).each {|i| outval << grp[i]}
        push_out(outval)
      end
      @groups = {}
    end
  end
  
  class PushArgAgg < PushGroup
    def initialize(elem_name, bud_instance, collection_name, keys_in, aggpairs_in, schema_in, &blk)
      raise "Multiple aggpairs #{aggpairs_in.map{|a| a.class.name}} in ArgAgg; only one allowed" if aggpairs_in.length > 1
      super(elem_name, bud_instance, collection_name, keys_in, aggpairs_in, schema_in, &blk)
      @agg = @aggpairs[0][0]
      @aggcol = @aggpairs[0][1]
      @winners = {}
    end
    
    def insert(item, source)
      key = @keys.map{|k| item[k]}
      @aggpairs.each_with_index do |ap, agg_ix|
        agg_input = item[ap[1]]
        if @groups[key].nil?
          agg = ap[0].send(:init, agg_input)
          @winners[key] = [item]
        else
          agg_result = ap[0].send(:trans, @groups[key][agg_ix], agg_input)
          agg = agg_result[0]
          case agg_result[1]
          when :ignore
            # do nothing
          when :replace
            @winners[key] = [item]
          when :keep
            @winners[key] << item 
          else
            raise "strange result from argagg finalizer" unless agg_result[1].class == Array and agg_result[1][0] == :delete
            agg_result[1][1..-1].each do |t|
              @winners[key].delete t unless @winners[key].nil?
            end
          end
        end
        @groups[key] ||= Array.new(@aggpairs.length)
        @groups[key][agg_ix] = agg
        push_out(nil)
      end      
    end

    def invalidate_cache
      @groups = {}
    end
    
    def local_flush#_end(source)
      @groups.keys.each {|g|
        @winners[g].each{|t|
          push_out(t, false)
        }
      }
      @groups = {}
    end
  end
end
