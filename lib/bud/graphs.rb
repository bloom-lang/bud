require 'rubygems'
require 'graphviz'

class GraphGen #:nodoc: all
  attr_reader :nodes

  def initialize(tableinfo, cycle, name, budtime, collapse=false, cardinalities={})
    @graph = GraphViz.new(:G, :type => :digraph, :label => "")
    @graph.node[:fontname] = "Times-Roman"
    @graph.node[:fontsize] = 18
    @graph.edge[:fontname] = "Times-Roman"
    @graph.edge[:fontsize] = 18
    @cards = cardinalities
    @name = name
    @collapse = collapse
    @budtime = budtime
    @internals = {'localtick' => 1, 'stdio' => 1}

    # map: table -> type
    @tabinf = {}
    tableinfo.each do |ti|
      @tabinf[ti[0].to_s] = ti[1]
    end

    @redcycle = {}
    cycle.each do |c|
      # assumption: !(c[2] and !c[3]), or stratification would have bombed out
      if c[2] and c[3]
        if !@redcycle[c[0]]
          @redcycle[c[0]] = []
        end
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
      head = d[1]
      body = d[3]

      # hack attack
      if @internals[head] or @internals[body]
        next
      end

      head = name_of(head)
      body = name_of(body)
      addonce(head, (head != d[1]), true)
      addonce(body, (body != d[3]))
      addedge(body, head, d[2], d[4], (head != d[1]), d[0])
    end
  end

  def addonce(node, negcluster, inhead=false)
    if !@nodes[node]
      @nodes[node] = @graph.add_node(node)
      if @cards and @cards[node]
        @nodes[node].label = node +"\n (#{@cards[node].to_s})"
      end
    end 

    if @budtime == -1
      @nodes[node].URL = "#{node}.html" if inhead
    else
      @nodes[node].URL = "javascript:openWin(\"#{node}\", #{@budtime})"
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
      @nodes[node].label = res
      @nodes[node].color = "red"
      @nodes[node].shape = "octagon"
      @nodes[node].penwidth = 3
      @nodes[node].URL = "#{File.basename(@name).gsub(".staging", "").gsub("collapsed", "expanded")}.svg"
    elsif @tabinf[node] and (@tabinf[node] == "Bud::BudTable")
      @nodes[node].shape = "rect"
    end
  end

  def addedge(body, head, op, nm, negcluster, rule_id=nil)
    return if body.nil? or head.nil?
    body = body.to_s
    head = head.to_s
    return if negcluster and body == head

    ekey = body + head
    if !@edges[ekey]
      @edges[ekey] = @graph.add_edge(@nodes[body], @nodes[head], :penwidth => 5)
      @edges[ekey].arrowsize = 2

      @edges[ekey].URL = "#{rule_id}.html" unless rule_id.nil?
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
      unless @nodes[name_of(k.to_s)] or k.to_s =~ /_tbl/ or @internals[k.to_s] or (k.to_s =~ /^t_/ and @budtime != 0)
        addonce(k.to_s, false)
      end
      if v == "Bud::BudPeriodic"
        addedge("S", k.to_s, false, false, false)
      end
    end

    unless depanalysis.nil? 
      depanalysis.source.each {|s| addedge("S", s.pred, false, false, false)}
      depanalysis.sink.each {|s| addedge(s.pred, "T", false, false, false)}

      unless depanalysis.underspecified.empty?
        addonce("??", false)
        @nodes["??"].color = "red"
        @nodes["??"].shape = "diamond"
        @nodes["??"].penwidth = 2
      end

      depanalysis.underspecified.each do |u|
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
      @graph.output(output => @name)
    end
  end
end

class SpaceTime    
  def initialize(input)
    @input = input 
    processes = input.map {|i| i[1]}
    input.map{|i| processes << i[2]}
    processes.uniq!

    @queues = {} 
    
    @g = GraphViz.new(:G, :type => :digraph, :rankdir => "LR", :outputorder => "edgesfirst")
    @hdr = @g.subgraph("cluster_0")
    
    @subs = {}
    @head = {}
    last = nil
    processes.each_with_index do |p, i|
      @head[p] = @hdr.add_node("process #{p}(#{i})")#, :color => "white", :label => "")
    end
  end

  def minn(a, b)
    a <= b ? a : b
  end

  def process
    # min of sender, receiver doesn't work
    #@input.sort {|a, b| minn(a[3], a[4]) <=> minn(b[3], b[4]) }.each do |i|
    # min sender
    @input.sort {|a, b| a[3] <=> b[3] }.each do |i|
      process_input(i)
    end
    
  end

  def process
    queues = {}
    @input.each do |i|
      queues[i[1]] = [] unless queues[i[1]]
      queues[i[2]] = [] unless queues[i[2]]
      queues[i[1]] << i[3]
      queues[i[2]] << i[4]
    end

    squeues = {}
    queues.each_pair do |k, v|
      squeues[k] = v.sort{|a, b| a <=> b}
    end
    
    todo = @input.clone
    # alg: add the edge with the lowest src timestamp, such that its src and dst 
    # are consistent with the local order along each timeline.
    while (todo.length > 0) 
      todo.sort{|a, b| a[3] <=> b[3]}.each do |i|
        if i[3] == squeues[i[1]].first and i[4] == squeues[i[2]].first
          process_input(i)
          squeues[i[1]].shift
          squeues[i[2]].shift
          todo.delete(i)
        end
      end
    end
  end
  
  def process_input(i)  
    snd_loc = i[1]
    rcv_loc = i[2]
    # node name used to be "#{snd_loc}-#{i[3]}"
    snd = @g.add_node("#{i[0].inspect}-#{snd_loc}-#{i[3]}", {:label => i[3].to_s, :width => 0.1, :height => 0.1, :fontsize => 6})
    rcv = @g.add_node("#{i[0].inspect}-#{rcv_loc}-#{i[3]}", {:label => i[4].to_s, :width => 0.1, :height => 0.1, :fontsize => 6})
    @g.add_edge(@head[snd_loc], snd, :weight => 8)
    @head[snd_loc] = snd
    @g.add_edge(@head[rcv_loc], rcv, :weight => 8)
    @head[rcv_loc] = rcv
    @g.add_edge(snd, rcv, :weight => 1, :label => i[0])
  end
  
  def finish(file)
    @g.output(:svg => "#{file}.svg")
  end
end
