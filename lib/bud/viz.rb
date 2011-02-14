require 'rubygems'
require 'syntax/convertors/html'
require 'gchart'
require 'digest/md5'
require 'bud/state'

class VizOnline
  include BudState
  def initialize(bud_instance)
    @bud_instance = bud_instance
    @meta_tables = {'t_rules' => 1, 't_depends' => 1, 't_table_info' => 1, 't_cycle' => 1, 't_stratum' => 1, 't_depends_tc' => 1}
    @bud_instance.options[:tc_dir] = "#{@bud_instance.class}_#{@bud_instance.port}_#{bud_instance.object_id}"
    @table_info = new_tab(:t_table_info, [:tab_name, :tab_type], @bud_instance)

    @logtab = {}
    @bud_instance.tables.each do |t|
      next if t[0].to_s =~ /_log\z/
      news = [:bud_time]
      t[1].schema.each do |s| 
        news << s 
      end
      lt = "#{t[0]}_log".to_sym
      @logtab[t[0]] = new_tab(lt, news, @bud_instance)
      @table_info << [t[0], t[1].class.to_s]
    end 
  end

  def new_tab(name, schema, instance)
      ret = Bud::BudTcTable.new(name, schema, instance)
      instance.tables[name] = ret
      return ret
  end

  def do_cards
    @bud_instance.tables.each do |t|
      tab = t[0]
      next if tab.to_s =~ /_log\z/
      next if @meta_tables[tab.to_s] and @bud_instance.budtime > 0
      t[1].each do |row|
        newrow = row.clone.unshift(@bud_instance.budtime)
        @logtab[tab] << newrow
      end
      @logtab[tab].tick
    end
  end
end
