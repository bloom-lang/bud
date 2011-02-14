# -*- coding: utf-8 -*-
require 'rubygems'
require 'graphviz'

class GraphGen

  def initialize(mapping, tableinfo, cycle, name, budtime, vizlevel, pics_dir, collapse=false, depanalysis=nil, cardinalities={})
    #@graph = GraphViz.new(:G, :type => :digraph, :label => "", :ratio => 0.85 )
    @graph = GraphViz.new(:G, :type => :digraph, :label => "")
    @graph.node[:fontname] = "Times-Roman"
    @graph.node[:fontsize] = 18
    @graph.edge[:fontname] = "Times-Roman"
    @graph.edge[:fontsize] = 18
    @tiers = []
    @cards = cardinalities
    @name = name
    @collapse = collapse
    @depanalysis = depanalysis
    @budtime = budtime
    @vizlevel = vizlevel
    @pics_dir = pics_dir
    @internals = {'count' => 1, 'localtick' => 1, 'stdio' => 1}

    # map: table -> stratum
    @t2s = {}
    #mapping.each_pair do |m,v|
    mapping.each do |m|
      puts "MAP: #{m.inspect}" 
      @t2s[m[0]] = m[1].to_i
    end

    # map: table -> type
    @tabinf = {}
    tableinfo.each do |ti|
      @tabinf[ti[0].to_s] = ti[1]
    end

    @redcycle = {}
    cycle.each do |c|
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
  
  def safe_t2s(tab)
    if @t2s[tab]
      @t2s[tab]
    else  
      words = tab.split(",")
      maxs = 0
      words.each do |w|
        if @t2s[w] and @t2s[w] > maxs
          maxs = @t2s[w]
        end
      end
      return maxs
    end
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
    # PAA
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

    #depends.each_pair do |d, v|
    depends.each do |d|
      puts "DEP: #{d.inspect}"
      head = d[1]
      body = d[3]

      # hack attack
      if @internals[head] or @internals[body]
        next
      end

      puts "get name of #{head}"
      head = name_of(head)
      body = name_of(body)
      addonce(head, (head != d[1]))
      addonce(body, (body != d[3]))
      addedge(body, head, d[2], d[4], (head != d[1]), d[0])
    end
  end

  def addonce(node, negcluster)
    puts "ADD NODE #{node}"
    if !@nodes[node]
      @nodes[node] = @graph.add_node(node)
      if @cards and @cards[node]
        #@nodes[node].label = node + "\n (<IMG SRC=\"" + @cards[node].to_s + "\">)"
        @nodes[node].label = node + "\n (#{@cards[node].to_s})"
        #@nodes[node].label = "<html>" + node + "<table><tr><td>1</td></tr></table> \n (#{@cards[node].to_s}) </html>"
        #@nodes[node].image = @cards[node].to_s 
        @nodes[node].imagescale = "both"
      end
  
      @nodes[node].URL = "#{output_dir}/#{node}_#{@budtime}.html" 
    end

    if negcluster
      # cleaning 
      res = node
      node.split(", ").each_with_index do |p, i|
        if i == 0
          res = p
        elsif i % 4 == 0
          res = res + ",\n" + p
        else
          res = res + ", " + p
        end
      end
      #@nodes[node].label = "<b>" + res + "</b>"
      @nodes[node].label = res
      @nodes[node].color = "red"
      @nodes[node].shape = "octagon"
      @nodes[node].penwidth = 3
      @nodes[node].URL = "file://#{ENV['PWD']}/#{@name}_expanded.svg"
    elsif @tabinf[node] and (@tabinf[node].class == Bud::BudTable)
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
      @edges[ekey] = @graph.add_edge(@nodes[body], @nodes[head])
      @edges[ekey].arrowsize = 2

      #@edges[ekey].URL = "file://#{ENV['PWD']}/plotter_out/#{rule_id}.html" unless rule_id.nil?
      @edges[ekey].URL = "#{output_dir}/#{rule_id}.html" unless rule_id.nil?
      if head =~ /_msg\z/
        @edges[ekey].minlen = 2
      else
        @edges[ekey].minlen = 1.5
      end
      @labels[ekey] = {}
      
    end

    #@edges[ekey].minlen = 5 if negcluster and body == head

    if op == '<+'
      @labels[ekey][' +/-'] = true
    elsif op == "<~"
      @edges[ekey].style = 'dashed'
    elsif op == "<-"
      #@labels[ekey] = @labels[ekey] + 'NEG(del)'
      @labels[ekey][' +/-'] = true
      @edges[ekey].arrowhead = 'veeodot'
    end
    if nm == 1 and head != "T"
      # hm, nonmono
      @edges[ekey].arrowhead = 'veeodot'
    end
  end

  def finish
    @labels.each_key do |k|
      @edges[k].label = @labels[k].keys.join(" ")
    end

    addonce("S", false)
    addonce("T", false)

    @nodes["S"].color = "blue"
    @nodes["T"].color = "blue"
    @nodes["S"].shape = "diamond"
    @nodes["T"].shape = "diamond"

    @nodes["S"].penwidth = 3
    @nodes["T"].penwidth = 3

    @tabinf.each_pair do |k, v|
      unless @nodes[name_of(k.to_s)] or k.to_s =~ /_tbl/ or @internals[k.to_s]
        addonce(k.to_s, false)
      end
    end

    unless @depanalysis.nil? or @depanalysis.underspecified.empty?
      @depanalysis.source.each {|s| addedge("S", s.pred, false, false, false) }
      @depanalysis.sink.each {|s| addedge(s.pred, "T", false, false, false) }
      addonce("??", false)
      @nodes["??"].color = "red"
      @nodes["??"].shape = "diamond"
      @nodes["??"].penwidth = 2

      @depanalysis.underspecified.each do |u|
        if u.input
          addedge(u.pred, "??", false, false, false)
        else
          addedge("??", u.pred, false, false, false)
        end
      end
    end

    suffix = @collapse ? "collapsed" : "expanded"
    @graph.output(:svg => "#{@name}_#{suffix}.svg")
    @graph.output(:dot => "#{@name}_#{suffix}.dot")
  end

  def output_base
    if @vizlevel >= 3
      @pics_dir
    else
      "plotter_out"
    end 
  end

  def output_dir
    "file://#{ENV['PWD']}/#{output_base}/"
  end

  def dump(shredded_rules)
    return if shredded_rules.nil?

    fout = File.new("#{output_base}/style.css", "w")
    fout.puts css
    fout.close

    code = {}
    rules = {}
    convertor = Syntax::Convertors::HTML.for_syntax "ruby"
    shredded_rules.each do |s|
      #fout = File.new("#{output_base}/#{s[0]}.html", "w+")
      fout = File.new("#{output_base}/#{s[0]}.html", "w+")
      fout.puts header
      fout.puts "<h1>Rule #{s[0]}</h1><br>"

      c = convertor.convert(s[3])
      c.sub!(/^<pre>/, "<pre class=\"code\">\n")
      fout.puts c
      rules[s[0]] = [s[1], s[3]]
      fout.close
    end

    rules.each_pair do |k, v|
      if !code[v[0]]
        code[v[0]] = ""
      end
      #code[v[0]] = "<br># RULE #{k}<br> " + code[v[0]] + "<br>" + v[1]
      code[v[0]] = "\n# RULE #{k}\n " + code[v[0]] + "\n" + v[1]
    end
    @nodes.each_pair do |k, v|
      fout = File.new("#{output_base}/#{k}.html", "w+")
      fout.puts header
      k.split(", ").each do |i|
        unless code[i].nil?
          c = convertor.convert(code[i])
          c.sub!(/^<pre>/, "<pre class=\"code\">\n")
          fout.puts c
        end
      end
      fout.puts("</body></html>")
      fout.close
    end
  end


  def header
      return "<html><meta content='text/html; charset=UTF-8' http-equiv='Content-Type'/>\n<head><link rel=\"stylesheet\" type=\"text/css\" href=\"style.css\" /></head><body>"
  end

  def css
    return "pre.code {
  padding: 1ex 1ex 1ex 1ex;
  border: 4px groove #CC0000;
  overflow-x: auto;
}

pre.code span.attribute { color: #009900; }
pre.code span.char { color: #F00; }
pre.code span.class { color: #A020F0; font-weight: bold; }
pre.code span.comment { color: #0000FF; }
pre.code span.constant { color: #008B8B; }
pre.code span.escape { color: #6A5ACD; }
pre.code span.expr { color: #2222CC; }
pre.code span.global { color: #11AA44; }
pre.code span.ident { color: #000000; }
pre.code span.keyword { color: #A52A2A; font-weight: bold; }
pre.code span.method { color: #008B8B; }
pre.code span.module { color: #A020F0; font-weight: bold; }
pre.code span.number { color: #DD00DD; }
pre.code span.punct { color: #6A5ACD; }
pre.code span.regex { color: #DD00DD; }
pre.code span.string { color: #DD00DD; }
pre.code span.symbol { color: #008B8B; }
"
  end

  
end
