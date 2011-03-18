require 'rubygems'
require 'bud'
require 'masterrtrace'

# Adds tracing to btquicksort
module RTraceBTQuicksort
  include MasterRTrace

  state do
    table :tc_succ_computation_delays, [:elt1, :elt2, :node] => [:time]
    table :tc_succ_communication_delays, [:elt1, :elt2, :node] => [:time]
    table :total_succ_computation_delay, [:elt1, :elt2] => [:time]
    table :total_succ_communication_delay, [:elt1, :elt2] => [:time]
  end

  bloom :compute_transitive_delays do
    # base case is the delay of succ_chan
    tc_succ_computation_delays <= base_computation_delay.map do |b|
      if b.pred == "succ_chan"
        [b.tuple[1], b.tuple[2], b.node, b.time]
      end
    end

    # transitive case is delay of the parent's elt_list_chan
    tc_succ_computation_delays <= join([tc_succ_computation_delays,
                                        base_computation_delay, tree_parent,
                                        node, node]) do |t, b, p, n1, n2|
      if t.node == n1.node and n1.uid == p.uid and n2.uid == p.parent_uid and
          n2.node == b.node and b.pred == "elt_list_chan"
        [t.elt1, t.elt2, b.node, b.time]
      end
    end

    total_succ_computation_delay <=
      tc_succ_computation_delays.group([:elt1, :elt2], sum(:time)).map do |t|
      [t[0], t[1], t[2]]
    end

    # ditto for communication, except this is from the perspective of the
    # receiver, and the master receives succ_chan, and the master doesn't have
    # any node as its parent, so we really need to convert it to be from the
    # perspective of the sender
    tc_succ_communication_delays <= join([base_communication_delay, send_time],
                                         [base_communication_delay.pred,
                                          send_time.pred],
                                         [base_communication_delay.tuple,
                                          send_time.tuple]) do |b, s|
      if b.pred == "succ_chan"
        [b.tuple[1], b.tuple[2], s.node, b.time]
      end
    end

    tc_succ_communication_delays <= join([tc_succ_communication_delays,
                                          base_communication_delay, tree_parent,
                                          node, node]) do |t, b, p, n1, n2|
      if t.node== n1.node and n1.uid == p.uid and n2.uid == p.parent_uid and
          n2.node == b.node and b.pred == "elt_list_chan"
        [t.elt1, t.elt2, b.node, b.time]
      end
    end

    total_succ_communication_delay <=
      tc_succ_communication_delays.group([:elt1, :elt2], sum(:time)).map do |t|
      [t[0], t[1], t[2]]
    end

  end
end
