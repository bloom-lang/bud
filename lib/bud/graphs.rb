require 'rubygems'
require 'graphviz'

class GraphGen #:nodoc: all
  attr_reader :nodes

  def initialize(tableinfo, builtin_tables, cycle, name, budtime, collapse=false, cardinalities={}, pathsto={}, begins={})
    @graph = GraphViz.new(:G, :type => :digraph, :label => "")
    #@graph.dim =  2
    @graph.node[:fontname] = "Times-Roman"
    @graph.node[:fontsize] = 18
    @graph.edge[:fontname] = "Times-Roman"
    @graph.edge[:fontsize] = 18
    @cards = cardinalities
    @name = name
    @collapse = collapse
    @budtime = budtime
    @builtin_tables = builtin_tables
    @pathsto = pathsto
    @begins = begins

    # map: table name -> type
    @tabinf = {}
    tableinfo.each do |ti|
      @tabinf[ti[0].to_s] = ti[1]
    end

    @redcycle = {}
    cycle.each do |c|
      # assumption: !(c[2] and !c[3]), or stratification would have bombed out
      if c[2] and c[3]
        @redcycle[c[0]] ||= []
        @redcycle[c[0]] << c[1]
      end
    end

    @nodes = {}
    @edges = {}
    @labels = {}
  end

  def name_bag(predicate, bag)
    if bag[predicate]
      return bag
    else
      bag[predicate] = true
      res = bag
      if @redcycle[predicate].nil?
        return res
      end
      @redcycle[predicate].each do |rp|
        res = name_bag(rp, res)
      end
    end

    return res
  end

  def name_of(predicate)
    # consider doing this in bud
    if @redcycle[predicate] and @collapse
      via = @redcycle[predicate]
      bag = name_bag(predicate, {})
      str = bag.keys.sort.join(", ")
      return str
    else
      return predicate
    end
  end

  def process(depends)
    # collapsing NEG/+ cycles.
    # we want to create a function from any predicate to (cycle_name or bottom)
    # bottom if the predicate is not in a NEG/+ cycle.  otherwise,
    # its name is "CYC" + concat(sort(predicate names))
    depends.each do |d|
      # b/c bud_obj was pruned before serialization...
      bud_obj, rule_id, lhs, op, body, nm, in_body = d.to_a
      head = lhs
      body = body

      if @builtin_tables.has_key?(head.to_sym) or @builtin_tables.has_key?(body.to_sym)
        next
      end

      head = name_of(head)
      body = name_of(body)
      addonce(head, (head != lhs), true)
      addonce(body, (body != body))
      addedge(body, head, op, nm, (head != lhs), rule_id)
    end
  end

  def color_node(paths)
    return "" if paths.nil?

    case paths[0][:val]
    when :A, :N
      "yellow"
    when :D, :G
      "red"
    else
      "black"
    end
  end

  def addonce(node, negcluster, inhead=false)
    if !@nodes[node]
      @nodes[node] = @graph.add_nodes(node)
      node_p = @nodes[node]
      node_p.label = node
      if @begins[:finish] and @begins[:finish][node]
        # point of divergence.  
        node_p.penwidth = 4
      end

      if @cards and @cards[node]
        node_p.label = "#{node}\n (#{@cards[node].to_s})"
        node_p.color = "green"
      else
        p = @pathsto[node].nil? ? "" : "\n(#{@pathsto[node][0][:val]})"
        node_p.label = node + p
        node_p.color = color_node(@pathsto[node])
      end
    else
      node_p = @nodes[node]
    end

    if @budtime == -1
      node_p.URL = "#{node}.html" if inhead
    else
      node_p.URL = "javascript:openWin(\"#{node}\", #{@budtime})"
    end

    if negcluster
      # cleaning
      res = node
      # pretty-printing issues
      node.split(", ").each_with_index do |p, i|
        if i == 0
          res = p
        elsif i % 4 == 0
          res = res + ",\n" + p
        else
          res = res + ", " + p
        end
      end
      node_p.label = res
      node_p.color = "red"
      node_p.shape = "octagon"
      node_p.penwidth = 3
      node_p.URL = "#{File.basename(@name).gsub(".staging", "").gsub("collapsed", "expanded")}.svg"
    elsif @tabinf[node] and (@tabinf[node] == "Bud::BudTable")
      node_p.shape = "rect"
    end
  end

  def addedge(body, head, op, nm, negcluster, rule_id=nil)
    return if body.nil? or head.nil?
    body = body.to_s
    head = head.to_s
    return if negcluster and body == head

    ekey = body + head
    if !@edges[ekey]
      @edges[ekey] = @graph.add_edges(@nodes[body], @nodes[head], :penwidth => 5)
      @edges[ekey].arrowsize = 2

      @edges[ekey].color = (@nodes[body]["color"].source || "")
      @edges[ekey].URL = "#{rule_id}-#{head}.html" unless rule_id.nil?
      if head =~ /_msg\z/
        @edges[ekey].minlen = 2
      else
        @edges[ekey].minlen = 1.5
      end
      @labels[ekey] = {}
    end

    if op == '<+'
      @labels[ekey][' +/-'] = true
    elsif op == "<~"
      @edges[ekey].style = 'dashed'
    elsif op == "<-"
      #@labels[ekey] = @labels[ekey] + 'NEG(del)'
      @labels[ekey][' +/-'] = true
      @edges[ekey].arrowhead = 'veeodot'
    end
    if nm and head != "T"
      # hm, nonmono
      @edges[ekey].arrowhead = 'veeodot'
    end
  end

  def finish(depanalysis=nil, output=nil)
    @labels.each_key do |k|
      @edges[k].label = @labels[k].keys.join(" ")
    end

    addonce("S", false)
    addonce("T", false)

    @nodes["T"].URL = "javascript:advanceTo(#{@budtime+1})"
    @nodes["S"].URL = "javascript:advanceTo(#{@budtime-1})"

    @nodes["S"].color = "blue"
    @nodes["T"].color = "blue"
    @nodes["S"].shape = "diamond"
    @nodes["T"].shape = "diamond"

    @nodes["S"].penwidth = 3
    @nodes["T"].penwidth = 3

    @tabinf.each_pair do |k, v|
      unless @nodes[name_of(k)] or @builtin_tables[k.to_sym]
        addonce(k, false)
      end
      if v == "Bud::BudPeriodic"
        addedge("S", k, false, false, false)
      end
    end

    unless depanalysis.nil?
      depanalysis.source.to_a.each {|s| addedge("S", s.pred, false, false, false)}
      depanalysis.sink.to_a.each {|s| addedge(s.pred, "T", false, false, false)}

      unless depanalysis.underspecified.empty?
        addonce("??", false)
        @nodes["??"].color = "red"
        @nodes["??"].shape = "diamond"
        @nodes["??"].penwidth = 2
      end

      depanalysis.underspecified.to_a.each do |u|
        if u.input
          addedge(u.pred, "??", false, false, false)
        else
          addedge("??", u.pred, false, false, false)
        end
      end
    end

    if output.nil?
      @graph.output(:svg => @name)
    else
      @graph.output(output.to_sym => @name)
    end
  end
end

class SpaceTime
  def initialize(input, links = false)
    @input = input
    @links = links
    processes = input.map {|i| i[1]}
    input.map{|i| processes << i[2]}
    processes.uniq!

    @queues = {}

    @g = GraphViz.new(:G, :type => :digraph, :rankdir => "LR", :outputorder => "nodesfirst", :splines => "line")#, :clusterrank => "none")
    @hdr = @g.subgraph("cluster_0")

    @subs = {}
    @head = {}
    last = nil
    processes.each_with_index do |p, i|
      @subs[p] = @g.subgraph("buster_#{i+1}")
      @head[p] = @hdr.add_nodes("process #{p}(#{i})", :group => p)#, :color => "white", :label => "")
    end
  end

  def msg_edge(f, t, l)
    lbl = "#{f}#{t}#{l}"
    if @edges[lbl]
      prev = @edges[lbl]
      @edges[lbl] = [prev[0], prev[1], prev[2], prev[3] + 1]
    else
      @edges[lbl] = [f, t, l, 1]
    end
  end

  def process
    @edges = {}
    queues = {}
    @input.each do |i|
      queues[i[1]] ||= []
      queues[i[2]] ||= []
      queues[i[1]] << i[3]
      queues[i[2]] << i[4]
    end

    squeues = {}
    queues.each_pair do |k, v|
      squeues[k] = v.sort{|a, b| a.to_i <=> b.to_i}
    end

    # create the nodes and the timeline edges first.
    squeues.each do |k, v|
      v.each_with_index do |item, i|
        label = "#{k}-#{item}"
        params = {:label => item.to_s, :width => 0.1, :height => 0.1, :fontsize => 6, :group => k}
        if @links
          params[:URL] = "DBM_#{k}/tm_#{item}.svg"
        end
        snd = @subs[k].add_nodes(label, params)
        unless @head[k].object_id == snd.object_id
          @subs[k].add_edges(@head[k], snd, :weight => 2)
          @head[k] = snd
        end
      end
    end

    #@input.sort{|a, b| a[3] <=> b[3]}.each do |i|
    @input.each do |i|
      snd_loc = i[1]
      rcv_loc = i[2]
      snd_label = "#{snd_loc}-#{i[3]}"
      rcv_label = "#{rcv_loc}-#{i[4]}"
      #@g.add_edge(snd_label, rcv_label, :color => "red", :weight => 1, :label => i[5])
      msg_edge(snd_label, rcv_label, i[5])
    end
  end

  def finish(file, fmt=nil)
    @edges.each_pair do |k, v|
      lbl = v[3] > 1 ? "#{v[2]}(#{v[3]})" : v[2]
      lbl ||= ""
      @g.add_edges(v[0], v[1], :label => lbl, :color => "red", :weight => 1)
    end
    if fmt.nil?
      @g.output(:svg => "#{file}.svg")
    else
      eval("@g.output(:#{fmt} => \"\#{file}.#{fmt}\")")
    end
  end
end
