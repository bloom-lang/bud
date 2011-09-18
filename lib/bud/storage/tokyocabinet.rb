begin
  require 'tokyocabinet'
  Bud::HAVE_TOKYOCABINET = true
rescue LoadError
end

module Bud
  # Persistent table implementation based on TokyoCabinet.
  class BudTcTable < BudCollection # :nodoc: all
    def initialize(name, bud_instance, given_schema)
      tc_dir = bud_instance.options[:tc_dir]
      raise BudError, "TC support must be enabled via 'tc_dir'" unless tc_dir
      if bud_instance.port.nil?
        raise BudError, "use of dbm storage requires an explicit port to be specified in Bud initialization options"
      end

      unless File.exists?(tc_dir)
        Dir.mkdir(tc_dir)
        puts "Created directory: #{tc_dir}" unless bud_instance.options[:quiet]
      end

      dirname = "#{tc_dir}/bud_#{bud_instance.port}"
      unless File.exists?(dirname)
        Dir.mkdir(dirname)
        puts "Created directory: #{dirname}" unless bud_instance.options[:quiet]
      end

      super(name, bud_instance, given_schema)
      @to_delete = []

      @hdb = TokyoCabinet::HDB.new
      db_fname = "#{dirname}/#{name}.tch"
      flags = TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT
      if bud_instance.options[:tc_truncate] == true
        flags |= TokyoCabinet::HDB::OTRUNC
      end
      if !@hdb.open(db_fname, flags)
        raise BudError, "failed to open TokyoCabinet DB '#{db_fname}': #{@hdb.errmsg}"
      end
      @hdb.tranbegin
    end

    def init_storage
      # XXX: we can't easily use the @storage infrastructure provided by
      # BudCollection; issue #33
      @storage = nil
    end

    def [](key)
      key_s = MessagePack.pack(key)
      val_s = @hdb[key_s]
      if val_s
        return make_tuple(key, MessagePack.unpack(val_s))
      else
        return @delta[key]
      end
    end

    def has_key?(k)
      key_s = MessagePack.pack(k)
      return true if @hdb.has_key? key_s
      return @delta.has_key? k
    end

    def include?(tuple)
      key = @key_colnums.map{|k| tuple[k]}
      value = self[key]
      return (value == tuple)
    end

    def make_tuple(k_ary, v_ary)
      t = Array.new(k_ary.length + v_ary.length)
      @key_colnums.each_with_index do |k,i|
        t[k] = k_ary[i]
      end
      val_cols.each_with_index do |c,i|
        t[schema.index(c)] = v_ary[i]
      end
      tuple_accessors(t)
    end

    def each(&block)
      each_from([@delta], &block)
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
      @hdb.each do |k,v|
        k_ary = MessagePack.unpack(k)
        v_ary = MessagePack.unpack(v)
        tick_metrics if bud_instance.options[:metrics]
        yield make_tuple(k_ary, v_ary)
      end
    end

    def flush
      @hdb.trancommit
    end

    def close
      @hdb.close
    end

    def merge_to_hdb(buf)
      buf.each do |key,tuple|
        merge_tuple(key, tuple)
      end
    end

    def merge_tuple(key, tuple)
      val = val_cols.map{|c| tuple[schema.index(c)]}
      key_s = MessagePack.pack(key)
      val_s = MessagePack.pack(val)
      if @hdb.putkeep(key_s, val_s) == false
        old_tuple = self[key]
        raise_pk_error(tuple, old_tuple) if tuple != old_tuple
      end
    end

    # move deltas to TC, and new_deltas to deltas
    def tick_deltas
      merge_to_hdb(@delta)
      @delta = @new_delta
      @new_delta = {}
    end

    superator "<-" do |o|
      o.each do |tuple|
        @to_delete << tuple unless tuple.nil?
      end
    end

    def insert(tuple)
      key = @key_colnums.map{|k| tuple[k]}
      merge_tuple(key, tuple)
    end

    alias << insert

    # Remove to_delete and then add pending to HDB
    def tick
      @to_delete.each do |tuple|
        k = @key_colnums.map{|c| tuple[c]}
        k_str = MessagePack.pack(k)
        cols_str = @hdb[k_str]
        unless cols_str.nil?
          hdb_cols = MessagePack.unpack(cols_str)
          delete_cols = val_cols.map{|c| tuple[schema.index(c)]}
          if hdb_cols == delete_cols
            @hdb.delete k_str
          end
        end
      end
      @to_delete = []

      merge_to_hdb(@pending)
      @pending = {}

      @hdb.trancommit
      @hdb.tranbegin
    end

    def method_missing(sym, *args, &block)
      @hdb.send sym, *args, &block
    end
  end
end
