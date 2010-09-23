require 'rubygems'
require 'graphviz'


class Viz 
  def initialize(strata, mapping, tableinfo)
    @graph = GraphViz.new(:G, :type => :digraph)
    @graph.node[:fontname] = "Times-Roman"
    @graph.edge[:fontname] = "Times-Roman"
    @tiers = []

    # array: strata
    (0..strata.first[0]+1).each do |s|
      @tiers[s] = @graph.subgraph("cluster" + s.to_s(), {:color => "black", :style => "dotted, rounded"})
    end

    # map: table -> stratum
    @t2s = {}
    mapping.each do |m|
      @t2s[m[0]] = m[1].to_i
    end


    @tabinf = {}
    tableinfo.each do |ti|
      #print "pop on #{ti[0].to_s}\n"
     # @tabinf[ti[0].to_s] = ti[1].class
      @tabinf[ti[0].to_s] = ti[1]
    end
    
    @nodes = {}
    @edges = {}
    @labels = {}
  end
  
  def safe_t2s(tab)
    if @t2s[tab]
      @t2s[tab]
    else  
      print "ST: safety\n"
      0
    end
  end

  def process(depends)
    @tabinf.each do |ti|
      print "TI: #{ti.inspect}\n"
    end
  
    depends.map{|de| de if @tabinf[de[0]] and @tabinf[de[2]]}.each do |d|
    #depends.map do |d|
      # why this is happening I do not know
      next if d.nil?
      print "OK d is #{d.inspect}\n"
      head = d[0]
      body = d[2]
      addonce(head)
      addonce(body)
      ekey = head + body
      # check if exists?
     
      #@edges[ekey] = @graph.add_edge(@nodes[head], @nodes[body])
      @edges[ekey] = @graph.add_edge(@nodes[body], @nodes[head])

      @labels[ekey] = ""

      print "GOT #{d.inspect}\n"

      if d[1] == '<+' 
        if @tabinf[head] == Bud::BudChannel
          # this is 'async'
          # but is it guarded?
          print "ASYNC #{head} (#{ekey})!!\n"
          @edges[ekey].style = 'dashed'
        else 
          # inductive
          @labels[ekey] = @labels[ekey] + '+'
        end  
      elsif d[1] == "<-"
        @labels[ekey] = @labels[ekey] + 'NEG(del)'
      end
      if d[3] == 1
        # hm, nonmono       
        @labels[ekey] = @labels[ekey] + 'NEG'
      end
    end
  end

  def addonce(node)
    print "ADDING\n"
    if !@nodes[node]
      print "ST is #{safe_t2s(node)} (with tiers #{@tiers.length})\n"
      @nodes[node] = @tiers[safe_t2s(node)].add_node(node)
    end
  end

  def finish(name)
    @labels.each_pair do |k, v|
      @edges[k].label = v
    end
    @graph.output(:pdf => "#{name}.pdf")
  end

  
end
