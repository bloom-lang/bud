require 'rubygems'
require 'bud/state'

class RTrace #:nodoc: all
  attr_reader :table_recv, :table_send, :table_sleep

  def initialize(bud_instance)
    @bud_instance = bud_instance
    return if bud_instance.class == Stratification or
      @bud_instance.class == DepAnalysis
    @table_recv = Bud::BudTable.new(:t_recv_time, @bud_instance, [:pred, :tuple, :time])
    @table_send = Bud::BudTable.new(:t_send_time, @bud_instance, [:pred, :tuple, :time])
    @table_sleep = Bud::BudTable.new(:t_sleep_time, @bud_instance, [:time])
  end

  def send(pred, datum)
    @table_send << [pred.to_s, datum, Time.now.to_f]
  end

  def recv(datum)
    @table_recv << [datum[0].to_s, datum[1], Time.now.to_f]
  end

  def sleep
    @table_sleep << [Time.now.to_f]
  end
end
