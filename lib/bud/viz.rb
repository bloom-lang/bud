require 'rubygems'
require 'syntax/convertors/html'
require 'gchart'
require 'digest/md5'
require 'bud/state'

class VizOnline #:nodoc: all
  include BudState

  def initialize(bud_instance)
    @bud_instance = bud_instance
    return if bud_instance.class == Stratification or @bud_instance.class == DepAnalysis
    @meta_tables = {'t_rules' => 1, 't_depends' => 1, 't_table_info' => 1, 't_cycle' => 1, 't_stratum' => 1, 't_depends_tc' => 1, 't_table_schema' => 1}
    @bud_instance.options[:tc_dir] = "TC_#{@bud_instance.class}_#{bud_instance.options[:tag]}_#{bud_instance.object_id}_#{bud_instance.port}"
    @table_info = new_tab(:t_table_info, [:tab_name, :tab_type], @bud_instance)
    @table_schema = new_tab(:t_table_schema, [:tab_name, :col_name, :ord], @bud_instance)

    @logtab = {}
    tmp_set = []
    @bud_instance.tables.each do |name, tbl|
      next if name.to_s =~ /_vizlog\z/

      # Temp collections don't have a schema until a fact has been inserted into
      # them; for now, we just include an empty schema for them in the viz
      if tbl.schema.nil?
        schema = []
      else
        schema = tbl.schema.clone
      end
      tmp_set << [name, schema, tbl.class.to_s]
    end

    tmp_set.each do |t|
      news = [:c_bud_time]
      @table_schema << [t[0], :c_bud_time, 0]
      t[1].each_with_index do |s, i|
        news << s
        @table_schema << [t[0], s, i+1]
      end
      lt = "#{t[0]}_vizlog".to_sym
      @logtab[t[0]] = new_tab(lt, news, @bud_instance)
      @table_info << [t[0], t[2]]
    end
  end

  def new_tab(name, schema, instance)
    ret = Bud::BudTcTable.new(name, instance, schema)
    instance.tables[name] = ret
    return ret
  end

  def do_cards
    return if @bud_instance.class == Stratification or @bud_instance.class == DepAnalysis
    @bud_instance.tables.each do |t|
      tab = t[0]
      next if tab.to_s =~ /_vizlog\z/
      next if @meta_tables[tab.to_s] and @bud_instance.budtime > 0
      next unless @logtab[tab]
      t[1].each do |row|
        newrow = [@bud_instance.budtime]
        row.each{ |r| newrow << r }
        @logtab[tab] << newrow
      end
      @logtab[tab].tick
    end
  end
end
