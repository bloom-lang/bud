require 'rubygems'
require 'bud/state'

class RTrace
  include BudState

  def initialize(bud_instance)
    @bud_instance = bud_instance
    return if bud_instance.class == Stratification or
      @bud_instance.class == DepAnalysis
    @table_recv = Bud::BudTable.new(:t_recv_time, @bud_instance, [:pred, :tuple, :time])
    @table_send = Bud::BudTable.new(:t_send_time, @bud_instance, [:pred, :tuple, :time])
    @table_sleep = Bud::BudTable.new(:t_slee_time, @bud_instance, [:time])
  end

  def send(pred, datum, time)
    @table_send << [pred.to_s, datum, time]
  end

  def recv(datum, time)
    @table_recv << [datum[0].to_s, datum[1], time]
  end

  def sleep(time)
    @table_sleep << [time]
  end
end
