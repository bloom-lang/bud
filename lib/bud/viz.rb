require 'rubygems'
require 'graphviz'


class Viz 
  def initialize(strata, mapping, tableinfo)
    @graph = GraphViz.new(:G, :type => :digraph)
    @graph.node[:fontname] = "Times-Roman"
    @graph.edge[:fontname] = "Times-Roman"
    @tiers = []

    # array: strata
    (0..strata.first[0]).each do |s|
      @tiers[s] = @graph.subgraph("cluster" + s.to_s(), {:color => "black", :style => "dotted, rounded"})
    end

    # map: table -> stratum
    @t2s = {}
    mapping.each do |m|
      @t2s[m[0]] = m[1].to_i
    end


    @tabinf = {}
    tableinfo.each do |ti|
      @tabinf[ti[0].to_s] = ti[1].class
    end
    
    @nodes = {}
    @edges = {}
    @labels = {}
  end

  def process(depends)
    depends.each do |d|
      head = d[0]
      body = d[2]
      addonce(head)
      addonce(body)
      ekey = head + body
      # check if exists?
      @edges[ekey] = @graph.add_edge(@nodes[head], @nodes[body])
      @labels[ekey] = ""

      if d[1] == '<' 
        t = @tabinf[head]
        print "TI[h] == #{@tabinf[head]}\n"
        if @tabinf[head] == Bud::BudChannel
          # this is 'async'
          # but is it guarded?
          print "ASYNC!!\n"
          @edges[ekey].style = 'dashed'
        else 
          print "INDUCT\n"
          # inductive
          @labels[ekey] = @labels[ekey] + '+'
        end  
      end
      if d[3] == 1
        # hm, nonmono       
        print "NM\n"
        @labels[ekey] = @labels[ekey] + 'NEG'
      end
    end
  end

  def addonce(node)
    if !@nodes[node]
      @nodes[node] = @tiers[@t2s[node]].add_node(node)
    end
  end

  def finish(name)
    @labels.each_pair do |k, v|
      @edges[k].label = v
    end
    @graph.output(:pdf => "#{name}.pdf")
  end

  
end
