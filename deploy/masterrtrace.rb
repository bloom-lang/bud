require 'rubygems'
require 'bud'
require 'countatomicdelivery'

module MasterRTrace
  include CountAtomicDelivery

  state do
    table :rtrace_master, [] => [:node]

    table :budtime_realtime_begin, [:node, :budtime] => [:realtime]
    table :budtime_realtime_end, [:node, :budtime] => [:realtime]

    # XXX: this assumes the exact same async fact is only ever received once by
    # each node
    table :recv_budtime, [:node, :pred, :tuple] => [:budtime]
    table :send_time, [:node, :pred, :tuple] => [:budtime, :realtime]
    table :base_computation_delay, [:node, :pred, :tuple] => [:time]
    table :base_communication_delay, [:node, :pred, :tuple] => [:time]
  end

  # aggregate all of the tracing information at a central location so we can
  # operate on it
  bloom :rtrace_send_to_master do
    atomic_data_in <= @rtracer and @rtracer.table_budtime_realtime_begin.map do |r|
      if rtrace_master[[]]
        [rtrace_master[[]].node, "budtime_realtime_begin", [me, r.budtime, r.realtime]]
      end
    end

    atomic_data_in <= @rtracer and @rtracer.table_budtime_realtime_end.map do |r|
      if rtrace_master[[]]
        [rtrace_master[[]].node, "budtime_realtime_end", [me, r.budtime, r.realtime]]
      end
    end

    atomic_data_in <= @rtracer and @rtracer.table_recv_budtime do |r|
      if rtrace_master[[]]
        [rtrace_master[[]].node, "recv_budtime", [me, r.pred, r.tuple, r.budtime]]
      end
    end

    atomic_data_in <= @rtracer and @rtracer.table_send_realtime do |r|
      if rtrace_master[[]]
        [rtrace_master[[]].node, "send_time", [me, r.pred, r.tuple, r.budtime, r.realtime]]
      end
    end

    bud_realtime_begin <= atomic_data_out.map do |a|
      a.tuple if a.bundle == "bud_realtime_begin"
    end

    bud_realtime_end <= atomic_data_out.map do |a|
      a.tuple if a.bundle == "bud_realtime_end"
    end

    recv_budtime <= atomic_data_out.map do |a|
      a.tuple if a.bundle == "recv_budtime"
    end

    send_time <= atomic_data_out.map do |a|
      a.tuple if a.bundle == "send_time"
    end

  end

  # this module computes the base cases; user writes app-specific stuff in a
  # separate aspect to deal with the inductive cases, since we don't want to be
  # tracking all the provenance information, and the user might not care about
  # all of this info anyway...
  bloom :compute_base_delays do
    # the base case of message computation delay: when it was sent minus when
    # the timestamp it was sent from began
    base_computation_delay <= join([bud_realtime_begin, send_time],
                                   [bud_realtime_begin.budtime,
                                    send_time.budtime],
                                   [bud_realtime_begin.node,
                                    send_time.node]).map do |b, s|
      [b.node, s.pred, s.tuple, s.realtime - b.realtime]
    end

    # same deal for communication delay: when it was sent minus when the
    # timestamp it was received at began
    base_communication_delay <= join([send_time, recv_budtime, bud_realtime],
                                     [send_time.pred, recv_budtime.pred],
                                     [send_time.tuple, recv_budtime.tuple],
                                     [recv_budtime.budtime,
                                      bud_realtime.budtime],
                                     [recv_budtime.node,
                                      bud_realtime.node]) do |s, r, b|
      [b.node, s.pred, s.tuple, b.realtime - s.realtime]
    end

  end

end
