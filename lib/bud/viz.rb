require 'rubygems'
require 'syntax/convertors/html'
require 'gchart'
require 'digest/md5'
require 'bud/state'

class VizOnline #:nodoc: all
  def initialize(bud_instance)
    @bud_instance = bud_instance
    return if bud_instance.class == Stratification or @bud_instance.class == DepAnalysis
    @meta_tables = {'t_rules' => 1, 't_depends' => 1, 't_table_info' => 1, 't_cycle' => 1, 't_stratum' => 1, 't_depends_tc' => 1, 't_table_schema' => 1}
    @bud_instance.options[:dbm_dir] = "DBM_#{@bud_instance.class}_#{bud_instance.options[:tag]}_#{bud_instance.object_id}_#{bud_instance.port}"
    @table_info = bud_instance.tables[:t_table_info]
    @table_schema = bud_instance.tables[:t_table_schema]
    @logtab = {}
    tmp_set = []
    @bud_instance.tables.each do |name, tbl|
      next if name.to_s =~ /_vizlog\z/
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
      @logtab[t[0]] = new_tab(lt, news, @bud_instance)
      if t[2] == "Bud::BudChannel"
        lts = "#{snd_alias}_vizlog".to_sym
        @logtab[snd_alias] = new_tab(lts, news, @bud_instance)
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
      end
      newrow = [@bud_instance.budtime]
      row.each{ |r| newrow << r }
      @logtab[tab] << newrow
    end
  end

  def do_cards
    return if @bud_instance.class == Stratification or @bud_instance.class == DepAnalysis
    @bud_instance.tables.each do |t|
      tab = t[0]
      next if tab.to_s =~ /_vizlog\z/
      next if @meta_tables[tab.to_s] and @bud_instance.budtime > 0
      next unless @logtab[tab]
      add_rows(t[1], tab)
      if t[1].class == Bud::BudChannel
        add_rows(t[1].pending, "#{tab}_snd")
      end
      @logtab[tab].tick
    end
  end
end
