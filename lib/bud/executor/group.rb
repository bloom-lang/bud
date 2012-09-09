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
      # An aggpair is an array: [agg class instance, array of indexes of input
      # agg input columns].  The second field is nil for Count.
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

      super(elem_name, bud_instance, collection_name, schema_in, &blk)
    end

    def insert(item, source)
      if @elim_dups
        return if @input_cache.include? item
        @input_cache << item
      end

      key = item.values_at(*@keys)
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
    end

    def flush
      # Don't emit fresh output unless a rescan is needed
      return unless @rescan
      @rescan = false

      @groups.each do |key, group_state|
        rv = key.clone
        @aggpairs.each_with_index do |ap, agg_ix|
          rv << ap[0].final(group_state[agg_ix])
        end
        push_out(rv)
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
      # Don't emit fresh output unless a rescan is needed
      return unless @rescan
      @rescan = false

      @groups.each_key do |g|
        @winners[g].each do |t|
          push_out(t)
        end
      end
    end
  end
end
