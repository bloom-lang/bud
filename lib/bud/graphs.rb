require 'rubygems'
require 'graphviz'

class GraphGen #:nodoc: all

  # PAA: 'mapping' already deprecated.
  # 'vizlevel' deprecated
  # 'depanalysis' deprecated (pushed to finish() method).
  #def initialize(mapping, tableinfo, cycle, name, budtime, vizlevel, pics_dir=nil, collapse=false, depanalysis=nil, cardinalities={})
  def initialize(tableinfo, cycle, name, budtime, pics_dir, collapse=false, cardinalities={})
    #@graph = GraphViz.new(:G, :type => :digraph, :label => "", :ratio => 0.85 )
    @graph = GraphViz.new(:G, :type => :digraph, :label => "")
    @graph.node[:fontname] = "Times-Roman"
    @graph.node[:fontsize] = 18
    @graph.edge[:fontname] = "Times-Roman"
    @graph.edge[:fontsize] = 18
    @cards = cardinalities
    @name = name
    @collapse = collapse
    @budtime = budtime
    @pics_dir = pics_dir
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
    depends.each do |d|
      head = d[1]
      body = d[3]

      # hack attack
      if @internals[head] or @internals[body]
        next
      end

      head = name_of(head)
      body = name_of(body)
      addonce(head, (head != d[1]))
      addonce(body, (body != d[3]))
      addedge(body, head, d[2], d[4], (head != d[1]), d[0])
    end
  end

  def addonce(node, negcluster)
    if !@nodes[node]
      @nodes[node] = @graph.add_node(node)
      if @cards and @cards[node]
        @nodes[node].label = node +"\n (#{@cards[node].to_s})"
      end
 
      if @pics_dir.nil?
        @nodes[node].URL = "#{node}.html"
      else
        @nodes[node].URL = "javascript:openWin(\"#{node}\", #{@budtime})"
      end
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
      @nodes[node].label = res
      @nodes[node].color = "red"
      @nodes[node].shape = "octagon"
      @nodes[node].penwidth = 3
      @nodes[node].URL = "#{File.basename(@name)}_expanded.svg"
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
    if nm and head != "T"
      # hm, nonmono
      @edges[ekey].arrowhead = 'veeodot'
    end
  end

  def finish(depanalysis=nil)
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

    suffix = @collapse ? "collapsed" : "expanded"
    fn = "#{@name}_#{suffix}.svg"
    staging = "#{fn}_staging"
    @graph.output(:svg => staging)
    fin = File.open(staging, "r")
    fout = File.open(fn, "w")
    while line = fin.gets
      fout.puts line.gsub("<title>G</title>", svg_javascript)
    end
    fin.close
    fout.close
    File.delete(staging) 
  end

  def output_base
    if @pics_dir.nil?
      "bud_doc"
    else
      @pics_dir
    end 
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
      fout = File.new("#{output_base}/#{s[0]}.html", "w+")
      fout.puts header
      fout.puts "<h1>Rule #{s[0]}</h1><br>"

      c = convertor.convert(s[3])
      c.sub!(/^<pre>/, "<pre class=\"code\" style='font-size:20px'>\n")
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
    return <<END_CSS
pre.code {
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
END_CSS
  end

  def svg_javascript
    return <<END_JS
<script type='text/javascript'>
  <![CDATA[

var windows = new Array()
var info = new Array()

function openWin(target, time) {
  win = window.open(target + \"_\" + time + \".html\", target, \"location=no,width=400,height=180,left=0,status=no\");
  // hm, an associative array, how strange.
  info[target] = 1
}

function advanceTo(time) {
  arr = gup(\"wins\").split(\",\");
  for (i=0; i < arr.length; i++) {
    if (arr[i] != \"\") {
      openWin(arr[i], time);
    }
  }
  str = '';
  // getting 'key_cols'
  for (var i in info) {
    str = str + ',' + i;
  }
  self.window.location.href = 'tm_' + time + '_expanded.svg?wins=' + str;
}

// off the netz
function gup(name) {
  name = name.replace(/[\[]/,\"\\\[\").replace(/[\]]/,\"\\\]\");
  var regexS = \"[\\?&]\"+name+\"=([^&#]*)\";
  var regex = new RegExp(regexS);
  var results = regex.exec(window.location.href);
  if (results == null)
    return \"\";
  else
    return results[1];
}

  ]]>
</script>
END_JS
  end
end
