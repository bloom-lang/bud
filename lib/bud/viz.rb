require 'bud/state'

class VizOnline #:nodoc: all
  attr_reader :logtab

  META_TABLES = %w[t_cycle t_depends t_provides t_rule_stratum t_rules t_stratum
                   t_table_info t_table_schema t_underspecified].to_set

  def initialize(bud_instance)
    @bud_instance = bud_instance
    @bud_instance.options[:dbm_dir] = "DBM_#{@bud_instance.class}_#{bud_instance.options[:tag]}_#{bud_instance.object_id}_#{bud_instance.port}"
    @table_info = bud_instance.tables[:t_table_info]
    @table_schema = bud_instance.tables[:t_table_schema]
    @logtab = new_tab(:the_big_log, [:table, :time, :contents], bud_instance)
    tmp_set = []
    @bud_instance.tables.each do |name, tbl|
      next if name == :the_big_log || name == :localtick
      # Temp collections don't have a schema until a fact has been inserted into
      # them; for now, we just include an empty schema for them in the viz
      if tbl.schema.nil?
        schema = [:a, :b, :c, :d]
      else
        schema = tbl.schema.clone
      end
      tmp_set << [name, schema, tbl.class.to_s]
    end

    tmp_set.each do |t|
      snd_alias = t[0].to_s + "_snd"
      @table_schema << [t[0], :c_bud_time, 0]
      t[1].each_with_index do |s, i|
        @table_schema << [t[0], s, i+1]
        if t[2] == "Bud::BudChannel"
          @table_schema << [snd_alias, s, i+1]
        end
      end
      if t[2] == "Bud::BudChannel"
        lts = "#{snd_alias}_vizlog".to_sym
        @table_info << [snd_alias, t[2]]
      end
      @table_info << [t[0], t[2]]
    end
  end

  def new_tab(name, schema, instance)
    ret = Bud::BudDbmTable.new(name, instance, schema)
    instance.tables[name] = ret
    return ret
  end

  def add_rows(collection, tab)
    collection.each do |row|
      if collection.class == Hash
        row = row[1]
      elsif collection.class == Bud::BudPeriodic
        row = row[0]
      end

      # t_depends, t_rule_stratum, and t_rules have Bud object as their first
      # field. Replace with a string, since Bud instances cannot be serialized.
      if row[0].class <= Bud
        row = row.to_a
        row = [row[0].class.to_s] + row[1..-1]
      end
      newrow = [tab, @bud_instance.budtime, row]
      begin
        @logtab << newrow
      rescue
        raise "ERROR!  #{@logtab} << #{newrow.inspect} (etxt #{$!})"
      end
    end
  end

  def do_cards
    @bud_instance.tables.each do |t|
      tab = t[0]
      next if tab == :the_big_log
      next if @bud_instance.budtime > 0 and META_TABLES.include? tab.to_s
      add_rows(t[1], tab)
      if t[1].class == Bud::BudChannel
        add_rows(t[1].pending, "#{tab}_snd")
      end
      @logtab.tick
    end
  end
end
