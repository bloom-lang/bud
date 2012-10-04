require 'rubygems'
require 'bud'
require 'graphviz'

# A simple interface between graphviz and bud
module BudGraph
  state do
    interface input, :bnode, [:name] => [:meta]
    interface input, :bedge, [:from, :to, :meta]
  end
end

module BloomGraph
  include BudGraph

  state do
    table :nodes, bnode.schema
    table :edges, bedge.schema
  end
  
  bloom do
    nodes <= bnode
    edges <= bedge
  end

  def finish(ignore, name, fmt=:pdf)
    it = ignore.to_set
    tick
    nodes.to_a.each do |n|
      unless it.include? n.name.to_sym
        @graph.add_nodes(n.name, n.meta)
      end
    end
  
    edges.to_a.each do |e|
      unless it.include? e.from.to_sym or it.include? e.to.to_sym
        @graph.add_edges(e.from, e.to, e.meta)
      end
    end
    @graph.output(fmt => name)
  end

  def initialize(opts={:type => :digraph})
    @graph = GraphViz.new(:G, opts)
    super
  end
end
