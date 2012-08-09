require 'bud/executor/elements'
require 'set'

module Bud
  class PushGroup < PushStatefulElement
    def initialize(elem_name, bud_instance, collection_name,
                   keys_in, aggpairs_in, schema_in, &blk)
      if keys_in.nil?
        @keys = []
      else
        @keys = keys_in.map{|k| k[1]}
      end
      # An aggpair is an array: [agg class instance, index of input field].
      # ap[1] is nil for Count.
      @aggpairs = aggpairs_in.map{|ap| [ap[0], ap[1].nil? ? nil : ap[1][1]]}
      @groups = {}

      # Check whether we need to eliminate duplicates from our input (we might
      # see duplicates because of the rescan/invalidation logic, as well as
      # because we don't do duplicate elimination on the output of a projection
      # operator). We don't need to dupelim if all the args are exemplary.
      @elim_dups = @aggpairs.any? {|a| not a[0].kind_of? ArgExemplary}
      if @elim_dups
        @input_cache = Set.new
      end

      @seen_new_data = false
      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end

    def insert(item, source)
      if @elim_dups
        return if @input_cache.include? item
        @input_cache << item
      end

      @seen_new_data = true
      key = @keys.map{|k| item[k]}
      group_state = @groups[key]
      if group_state.nil?
        @groups[key] = @aggpairs.map do |ap|
          input_val = ap[1].nil? ? item : item[ap[1]]
          ap[0].init(input_val)
        end
      else
        @aggpairs.each_with_index do |ap, agg_ix|
          input_val = ap[1].nil? ? item : item[ap[1]]
          state_val = ap[0].trans(group_state[agg_ix], input_val)[0]
          group_state[agg_ix] = state_val
        end
      end
    end

    def add_rescan_invalidate(rescan, invalidate)
      # XXX: need to understand why this is necessary; it is dissimilar to the
      # way other stateful non-monotonic operators are handled.
      rescan << self
      super
    end

    def invalidate_cache
      puts "#{self.class}/#{self.tabname} invalidated" if $BUD_DEBUG
      @groups.clear
      @input_cache.clear if @elim_dups
      @seen_new_data = false
    end

    def flush
      # If we haven't seen any input since the last call to flush(), we're done:
      # our output would be the same as before.
      return unless @seen_new_data
      @seen_new_data = false

      @groups.each do |g, grps|
        grp = @keys == $EMPTY ? [[]] : [g]
        @aggpairs.each_with_index do |ap, agg_ix|
          grp << ap[0].final(grps[agg_ix])
        end
        outval = grp[0].flatten
        (1..grp.length-1).each {|i| outval << grp[i]}
        push_out(outval)
      end
    end
  end

  class PushArgAgg < PushGroup
    def initialize(elem_name, bud_instance, collection_name, keys_in, aggpairs_in, schema_in, &blk)
      unless aggpairs_in.length == 1
        raise Bud::Error, "multiple aggpairs #{aggpairs_in.map{|a| a.class.name}} in ArgAgg; only one allowed"
      end
      super(elem_name, bud_instance, collection_name, keys_in, aggpairs_in, schema_in, &blk)
      @agg, @aggcol = @aggpairs[0]
      @winners = {}
    end

    public
    def invalidate_cache
      super
      @winners.clear
    end

    def insert(item, source)
      key = @keys.map{|k| item[k]}
      group_state = @groups[key]
      if group_state.nil?
        @seen_new_data = true
        @groups[key] = @aggpairs.map do |ap|
          @winners[key] = [item]
          input_val = item[ap[1]]
          ap[0].init(input_val)
        end
      else
        @aggpairs.each_with_index do |ap, agg_ix|
          input_val = item[ap[1]]
          state_val, flag, *rest = ap[0].trans(group_state[agg_ix], input_val)
          group_state[agg_ix] = state_val
          @seen_new_data = true unless flag == :ignore

          case flag
          when :ignore
            # do nothing
          when :replace
            @winners[key] = [item]
          when :keep
            @winners[key] << item
          when :delete
            rest.each do |t|
              @winners[key].delete t
            end
          else
            raise Bud::Error, "strange result from argagg transition func: #{flag}"
          end
        end
      end
    end

    def flush
      # If we haven't seen any input since the last call to flush(), we're done:
      # our output would be the same as before.
      return unless @seen_new_data
      @seen_new_data = false

      @groups.each_key do |g|
        @winners[g].each do |t|
          push_out(t, false)
        end
      end
    end
  end
end
