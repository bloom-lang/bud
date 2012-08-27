require 'dbm'

module Bud
  # Persistent table implementation based on dbm.
  class BudDbmTable < BudPersistentCollection # :nodoc: all
    def initialize(name, bud_instance, given_schema)
      dbm_dir = bud_instance.options[:dbm_dir]
      raise Bud::Error, "dbm support must be enabled via 'dbm_dir'" unless dbm_dir
      if bud_instance.port.nil?
        raise Bud::Error, "use of dbm storage requires an explicit port to be specified in Bud initialization options"
      end

      unless File.exists?(dbm_dir)
        Dir.mkdir(dbm_dir)
        puts "Created directory: #{dbm_dir}" unless bud_instance.options[:quiet]
      end
      dirname = "#{dbm_dir}/bud_#{bud_instance.port}"
      unless File.exists?(dirname)
        Dir.mkdir(dirname)
        puts "Created directory: #{dirname}" unless bud_instance.options[:quiet]
      end

      super(name, bud_instance, given_schema)
      @to_delete = []
      @invalidated = true

      db_fname = "#{dirname}/#{name}.dbm"
      flags = DBM::WRCREAT
      if bud_instance.options[:dbm_truncate] == true
        flags |= DBM::NEWDB
      end
      @dbm = DBM.open(db_fname, 0666, flags)
      if @dbm.nil?
        raise Bud::Error, "failed to open dbm database '#{db_fname}': #{@dbm.errmsg}"
      end
    end

    def init_storage
      # XXX: we can't easily use the @storage infrastructure provided by
      # BudCollection; issue #33
      @storage = nil
    end

    def [](key)
      check_enumerable(key)
      key_s = MessagePack.pack(key)
      val_s = @dbm[key_s]
      if val_s
        return make_tuple(key, MessagePack.unpack(val_s))
      else
        return @delta[key]
      end
    end

    def length
      @dbm.length + @delta.length
    end

    def has_key?(k)
      check_enumerable(k)
      key_s = MessagePack.pack(k)
      return true if @dbm.has_key? key_s
      return @delta.has_key? k
    end

    def include?(tuple)
      key = get_key_vals(tuple)
      value = self[key]
      return (value == tuple)
    end

    def make_tuple(k_ary, v_ary)
      t = @struct.new
      @key_colnums.each_with_index do |k,i|
        t[k] = k_ary[i]
      end
      val_cols.each_with_index do |c,i|
        t[cols.index(c)] = v_ary[i]
      end
      t
    end

    def each(&block)
      each_from([@delta], &block)
      each_storage(&block)
    end

    def each_raw(&block)
      each_storage(&block)
    end

    def each_from(bufs, &block)
      bufs.each do |b|
        if b == @storage then
          each_storage(&block)
        else
          b.each_value do |v|
            tick_metrics if bud_instance.options[:metrics]
            yield v
          end
        end
      end
    end

    def each_storage(&block)
      @dbm.each do |k,v|
        k_ary = MessagePack.unpack(k)
        v_ary = MessagePack.unpack(v)
        tick_metrics if bud_instance.options[:metrics]
        yield make_tuple(k_ary, v_ary)
      end
    end

    def flush
    end

    def close
      @dbm.close unless @dbm.nil?
      @dbm = nil
    end

    def merge_to_db(buf)
      buf.each do |key,tuple|
        merge_tuple_to_db(key, tuple)
      end
    end

    def merge_tuple_to_db(key, tuple)
      key_s = MessagePack.pack(key)
      if @dbm.has_key?(key_s)
        old_tuple = self[key]
        raise_pk_error(tuple, old_tuple) if tuple != old_tuple
      else
        val = val_cols.map{|c| tuple[cols.index(c)]}
        @dbm[key_s] = MessagePack.pack(val)
      end
    end

    # move deltas to on-disk storage, and new_deltas to deltas
    def tick_deltas
      unless @delta.empty?
        merge_to_db(@delta)
        @tick_delta.concat(@delta.values) if accumulate_tick_deltas
        @delta.clear
      end
      unless @new_delta.empty?
        # We allow @new_delta to contain duplicates but eliminate them here. We
        # can't just allow duplicate delta tuples because that might cause
        # spurious infinite delta processing loops.
        @new_delta.reject! {|key, val| self[key] == val}

        @delta = @new_delta
        @new_delta = {}
      end
      return !(@delta.empty?)
    end

    public
    def flush_deltas
      unless @delta.empty?
        merge_to_db(@delta)
        @tick_delta.concat(@delta.values) if accumulate_tick_deltas
        @delta.clear
      end
      merge_to_db(@new_delta)
      @new_delta = {}
    end

    # This is verbatim from BudTable.  Need to DRY up.  Should we be a subclass
    # of BudTable?
    public
    def pending_delete(o)
      if o.class <= Bud::PushElement
         o.wire_to(self, :delete)
      elsif o.class <= Bud::BudCollection
        o.pro.wire_to(self, :delete)
      else
        @to_delete.concat(o.map{|t| prep_tuple(t) unless t.nil?})
      end
    end
    superator "<-" do |o|
      pending_delete(o)
    end

    def insert(tuple)
      key = get_key_vals(tuple)
      merge_tuple_to_db(key, tuple)
    end

    alias << insert

    # Remove to_delete and then move pending => delta.
    def tick
      deleted = nil
      @to_delete.each do |tuple|
        k = get_key_vals(tuple)
        k_str = MessagePack.pack(k)
        cols_str = @dbm[k_str]
        unless cols_str.nil?
          db_cols = MessagePack.unpack(cols_str)
          delete_cols = val_cols.map{|c| tuple[cols.index(c)]}
          if db_cols == delete_cols
            deleted ||= @dbm.delete k_str
          end
        end
      end
      @to_delete = []

      @invalidated = !deleted.nil?
      unless @pending.empty?
        @delta = @pending
        @pending = {}
      end
      flush
    end

    def invalidate_cache
    end

    # XXX: shouldn't this check @delta as well?
    public
    def empty?
      @dbm.empty?
    end
  end
end
