require 'rubygems'
require 'syntax/convertors/html'
require 'gchart'
require 'bud/state'

class VizOnline #:nodoc: all
  attr_reader :logtab
  def initialize(bud_instance)
    @bud_instance = bud_instance
    @meta_tables = {'t_rules' => 1, 't_depends' => 1, 't_table_info' => 1, 't_cycle' => 1, 't_stratum' => 1, 't_depends_tc' => 1, 't_table_schema' => 1, 't_provides' => 1}
    @bud_instance.options[:dbm_dir] = "DBM_#{@bud_instance.class}_#{bud_instance.options[:tag]}_#{bud_instance.object_id}_#{bud_instance.port}"
    @table_info = bud_instance.tables[:t_table_info]
    @table_schema = bud_instance.tables[:t_table_schema]
    @logtab = new_tab("the_big_log", [:table, :time, :contents], bud_instance)
    tmp_set = []
    @bud_instance.tables.each do |name, tbl|
      next if name == "the_big_log"
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
      news = [:c_bud_time]
      snd_alias = t[0].to_s + "_snd"
      @table_schema << [t[0], :c_bud_time, 0]
      t[1].each_with_index do |s, i|
        news << s
        @table_schema << [t[0], s, i+1]
        if t[2] == "Bud::BudChannel"
          @table_schema << [snd_alias, s, i+1]
        end
      end
      lt = "#{t[0]}_vizlog".to_sym
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

      # bud.t_depends and t_rules have bud object in field[0]. Remove them since
      # bud instances cannot/must not be serialized.
      if row[0].class <= Bud
        row = row.to_a if row.class != Array
        row = [row[0].class.to_s] + row[1..-1] if row[0].class <= Bud
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
      next if tab == "the_big_log"
      next if @meta_tables[tab.to_s] and @bud_instance.budtime > 0
      # PAA: why did we previously exclude periodics?
      add_rows(t[1], tab) #####unless t[1].class == Bud::BudPeriodic
      if t[1].class == Bud::BudChannel
        add_rows(t[1].pending, "#{tab}_snd")
      end
      @logtab.tick
    end
  end
end
