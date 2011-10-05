require 'rubygems'
require 'bud/graphs'

module TraceCardinality
  state do
    table :full_info, [:bud_time, :table, :row]
    scratch :cardinalities, [:bud_time, :table] => [:cnt]
    scratch :times, [:bud_time]
  end

  bloom do
    cardinalities <= full_info.group([full_info.bud_time, full_info.table], count)
    times <= full_info {|f| [f.bud_time]}
  end
end

class VizHelper
  include Bud
  include TraceCardinality

  def initialize(tabinf, cycle, depends, rules, dir)
    @t_tabinf = tabinf
    @t_cycle = cycle
    @t_depends = depends
    @t_rules = rules
    @dir = dir
    super()
  end

  def summarize(dir, schema)
    table_io = {}
    cardinalities.sort{|a, b| a[0] <=> b[0]}.each do |card|
      table_io["#{card.table}_#{card.bud_time}"] = start_table(dir, card.table, card.bud_time, schema[card.table])
    end

    full_info.each do |info|
      write_table_content(table_io["#{info.table}_#{info.bud_time}"], info.row)
    end

    table_io.each_value do |tab|
      end_table(tab)
    end

    # fix: nested loops
    times.sort.each do |time|
      card_info = {}
      cardinalities.each do |card|
        if card.bud_time == time.bud_time
          card_info[card.table] = card.cnt
        end
      end

      d = "#{@dir}/tm_#{time.bud_time}"
      write_graphs(@t_tabinf, @t_cycle, @t_depends, @t_rules, d, @dir, nil, false, nil, time.bud_time, card_info)
    end
  end

end


module VizUtil #:nodoc: all
  def graph_from_instance(bud_instance, viz_name, output_base, collapse=true, fmt=nil)
    tabinf = {}
    bud_instance.tables.each do |t|
      tab = t[0].to_s
      tabinf[tab] = t[1].class.to_s
    end
    write_graphs(tabinf, bud_instance.t_cycle, bud_instance.t_depends, bud_instance.t_rules, viz_name, output_base, fmt, collapse, bud_instance.meta_parser.depanalysis)
  end

  def write_graphs(tabinf, cycle, depends, rules, viz_name, output_base, fmt, collapse, depanalysis=nil, budtime=-1, card_info=nil)
    staging = "#{viz_name}.staging"
    gv = GraphGen.new(tabinf, cycle, staging, budtime, collapse, card_info)
    gv.process(depends)
    dump(rules, output_base, gv)
    gv.finish(depanalysis, fmt)
    inject_js(staging, "#{viz_name}.svg")
    File.delete(staging)
  end

  def inject_js(input, fname)
    fin = File.open(input, "r")
    fout = File.open(fname, "w")
    while line = fin.gets
      fout.puts line.gsub("<title>G</title>", svg_javascript)
    end
    fin.close
    fout.close
  end

  def dump(shredded_rules, output_base, gv_obj)
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

      c = convertor.convert(s[4])
      c.sub!(/^<pre>/, "<pre class=\"code\" style='font-size:20px'>\n")
      fout.puts c
      rules[s[0]] = [s[1], s[4]]
      fout.close
    end

    rules.each_pair do |k, v|
      if !code[v[0]]
        code[v[0]] = ""
      end
      #code[v[0]] = "<br># RULE #{k}<br> " + code[v[0]] + "<br>" + v[1]
      code[v[0]] = "\n# RULE #{k}\n " + code[v[0]] + "\n" + v[1]
    end
    gv_obj.nodes.each_pair do |k, v|
      fout = File.new("#{output_base}/#{k[0..55]}.html", "w+")
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
  self.window.location.href = 'tm_' + time + '.svg?wins=' + str;
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

  def get_meta2(dir)
    meta_tabs = {"t_table_info" => :tabinf, "t_table_schema" => :tabscm, "t_cycle" => :cycle, "t_depends" => :depends, "t_rules" => :rules}
    meta = {}
    data = []

    
    dir = Dir.glob("#{dir}/bud*").first

    ret = DBM.open("#{dir}/the_big_log.dbm")
    ret.each_pair do |k, v|
      key = MessagePack.unpack(k)
      tab = key.shift
      time = key.shift
      # paa
      tup = key[0]
      MessagePack.unpack(v).each {|val| tup << val}
      if meta_tabs[tab]
        raise "non-zero budtime.  sure this is metadata?" if time != 0 and strict
        meta[meta_tabs[tab]] ||= []
        meta[meta_tabs[tab]] << tup
        #ret << tup
      else
        data << [time, tab, tup]
      end
    end
    
    meta_tabs.each_value do |tab|
      meta[tab] ||= [] 
    end

    meta[:schminf] = {}
    meta[:tabscm].each do |ts|
      tab = ts[0].to_s
      unless meta[:schminf][tab]
        meta[:schminf][tab] = []
      end
      meta[:schminf][tab][ts[2]] = ts[1]
    end
    return meta, data
  end

  def start_table(dir, tab, time, schema)
    str = "#{dir}/#{tab}_#{time}.html"
    fout = File.new(str, "w")

    fout.puts "<html><title>#{tab} @ #{time}</title>"
    fout.puts "<table border=1>"
    fout.puts "<tr>" + schema.map{|s| "<th> #{s} </th>"}.join(" ") + "<tr>" unless schema.nil?
    fout.close
    return str
  end

  def end_table(stream)
    fp = File.open(stream, "a")
    fp.puts "</table>"
    fp.close
  end

  def write_table_content(fn, row)
    stream = File.open(fn, "a")
    stream.puts "<tr>"
    stream.puts row.map{|c| "<td>#{c.to_s}</td>"}.join(" ")
    stream.puts "</tr>"
    stream.close
  end

end
