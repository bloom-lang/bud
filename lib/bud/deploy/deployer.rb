require 'rubygems'
require 'bud'
require 'bud/deploy/countatomicdelivery'

class Module
  def deploystrap(&block)
    meth_name = "__deploystrap__#{Module.get_class_name(self)}".to_sym
    define_method(meth_name, &block)
  end
end

module Deployer # :nodoc: all
  include CountAtomicDelivery

  state do
    table :node, [:uid] => [:addr]
    table :node_count, [] => [:num]
    # At the deployer node, this collection will contain a fact for the first
    # tick in which the given node is ready
    scratch :node_ready, [:uid]

    table :initial_data, [:uid, :pred, :data]
    channel :dont_care, [:@loc]
    table :dead, [:dead]
  end

  def depl_idempotent(r) (dead.include? r) ? false : dead.insert(r) end

  def do_deploystrap
    self.class.ancestors.each do |anc|
      anc.instance_methods(false).each do |m|
        if /^__deploystrap__/.match m
          self.method(m.to_sym).call
        end
      end
    end
  end

  def initialize(opt={})
    super
    do_deploystrap if opt[:deploy]
  end

  # Distribute the EDB to each node.
  #
  # XXX: this may break coordination protocols that assume the EDB is present
  # before any messages are received.  In order to fix this, we would probably
  # need to globally synchronize to ensure that "timestamp 0" gets "fully
  # evaluated" before any messages can be sent.
  bloom :distribute_data do
    atomic_data_in <= (node_ready * node * initial_data).combos(node_ready.uid => node.uid, node.uid => initial_data.uid) do |nr, n, i|
      [n.addr, [i.pred, i.data]]
    end

    # Add all tuples at once.
    dont_care <~ atomic_data_out do |a|
      a.tuple[1].map do |d|
        eval a.tuple[0].to_s + " <+ [" + d.inspect + "]"
      end
      [ip_port]
    end
  end

  bloom :print_ready do
    stdio <~ node_ready {|nr| ["Child node ready: #{nr.uid}"]}
  end
end
