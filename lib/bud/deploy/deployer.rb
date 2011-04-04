require 'rubygems'
require 'bud'
require 'open-uri'
require 'bud/deploy/countatomicdelivery'

module Deployer # :nodoc: all
  include CountAtomicDelivery

  state do
    table :node, [:uid] => [:node] # nodes with running programs
    table :node_count, [] => [:num]
    table :initial_data, [:uid, :pred, :data]
    channel :dont_care, [:@loc]
    table :dead, [:dead]
  end

  def idempotent(r) (dead.include? r) ? false : dead.insert(r) end

  def initialize(opt)
    super
    if opt[:deploy]
      deploystrap
    end
  end

  # distribute the EDB to each node
  #
  # XXX: this may break coordination protocols that assume the EDB is present
  # before any messages are received.  In order to fix this, we would probably
  # need to globally synchronize to ensure that "timestamp 0" gets "fully
  # evaluated" before any messages can be sent

  bloom :distribute_data do
    atomic_data_in <= join([node, initial_data],
                           [node.uid, initial_data.uid]).map do |n, i|
      puts "sending EDB to " + n.inspect
      [n.node, [i.pred, i.data]] if idempotent [[n.node, i.pred, i.data]]
    end

    # add all tuples at once
    dont_care <~ atomic_data_out.map do |a|
      if idempotent a
        a.tuple[1].map do |d|
          eval a.tuple[0].to_s + " <+ [" + d.inspect + "]"
        end
        [ip_port]
      end
    end

  end
end
