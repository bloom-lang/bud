require 'rubygems'
require 'bud/graphs'
require 'bud/meta_algebra'
require 'syntax/convertors/html'

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

  def initialize(tabinf, cycle, depends, rules, dir, provides)
    @t_tabinf = tabinf
    @t_cycle = cycle
    @t_depends = depends
    @t_rules = rules
    @t_provides = provides
    @dir = dir
    super()
  end

  def summarize(dir, schema)
    table_io = {}
    cardinalities.to_a.sort{|a, b| a[0] <=> b[0]}.each do |card|
      table_io["#{card.table}_#{card.bud_time}"] = start_table(dir, card.table, card.bud_time, schema[card.table])
    end

    full_info.each do |info|
      write_table_content(table_io["#{info.table}_#{info.bud_time}"], info.row)
    end

    table_io.each_value do |tab|
      end_table(tab)
    end

    # fix: nested loops
    times.to_a.sort.each do |time|
      card_info = {}
      cardinalities.each do |card|
        if card.bud_time == time.bud_time
          card_info[card.table] = card.cnt
        end
      end

      d = "#{@dir}/tm_#{time.bud_time}"
      write_graphs(@t_tabinf, builtin_tables, @t_cycle, @t_depends, @t_rules, d,
                   @dir, nil, false, nil, time.bud_time, card_info)
    end
  end
end


module VizUtil #:nodoc: all
  def self.ma_tables
    # a craven, contemptible hack to grab the metatables.
    estr = "class Foo\ninclude Bud\ninclude MetaAlgebra\ninclude MetaReports\nend"
    eval(estr)
    e = Foo.new
    e.tables
  end

  def graph_from_instance(bud_instance, viz_name, output_base, collapse=true, fmt=nil, data=nil)
    tabinf = {}
    bud_instance.tables.each do |t|
      tab = t[0].to_s
      tabinf[tab] = t[1].class.to_s
    end

    begins = get_paths(bud_instance)
    bit = bud_instance.builtin_tables 
    VizUtil.ma_tables.each_pair{|k, v| bit[k] = v}

    depanalysis =  bud_instance.meta_parser.depanalysis
    write_graphs(tabinf, bit, depanalysis.cycle,
                 bud_instance.t_depends, bud_instance.t_rules, viz_name,
                 output_base, fmt, collapse, depanalysis, -1, nil,
                 get_labels(bud_instance), begins)
    begins
  end

  def get_paths(bud_instance)
    return {} unless bud_instance.respond_to? :a_preds 
    begins = {}
    bud_instance.a_preds.each do |b|
      begins[:start] ||= {}
      begins[:start][b.path.split("|").last] = b.fullpath.split("|").last
      begins[:finish] = {}
      begins[:finish][b.fullpath.split("|").last] = true
    end
    begins
  end

  def get_labels(bud_instance)
    return {} unless bud_instance.respond_to? :lps
    # sort the paths.  sort the paths to the same destination by length.
    aps = {}
    ap_interm = bud_instance.lps.to_a.sort do |a, b|
      if a.to == b.to then
        a.path.length <=> b.path.length
      else
        a <=> b
      end
    end
    ap_interm.each do |a|
      aps[a.to] ||= []
      aps[a.to] << a.tag
    end

    # grab the lattice metadata
    lub = {}
    bud_instance.lub.each do |l|
      lub[l.left] ||= {}
      lub[l.left][l.right] = l.result
    end

    # b/c set union isn't working right
    ap2 = {}
    aps.each_pair do |k, v|
      tmp = v.reduce({}) do |memo, i|
        memo[:val] ||= :M
        was = memo[:val]
        if lub[memo[:val]][i]
          if i == :N
            memo[:val] = lub[i][memo[:val]]
          else
            memo[:val] = lub[memo[:val]][i]
          end
        else
          puts "couldn't find #{memo[:val]} - #{i} in #{lub.inspect}"
        end
        memo
      end
      ap2[k] = [tmp, v]
    end
    ap2
  end

  def write_graphs(tabinf, builtin_tables, cycle, depends, rules, viz_name,
                   output_base, fmt, collapse, depanalysis=nil, budtime=-1,
                   card_info=nil, pathsto={}, begins={})
    staging = "#{viz_name}.staging"
    gv = GraphGen.new(tabinf, builtin_tables, cycle, staging, budtime,
                      collapse, card_info, pathsto, begins)
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
      # b/c accessors don't make it through serialization anymore
      bud_obj, rule_id, lhs, op, src, orig_src, unsafe_funcs_called = s.to_a
      fout = File.new("#{output_base}/#{rule_id}-#{lhs}.html", "w+")
      fout.puts header
      fout.puts "<h1>Rule #{rule_id}</h1><br>"

      c = convertor.convert(orig_src)
      c.sub!(/^<pre>/, "<pre class=\"code\" style='font-size:20px'>\n")
      fout.puts c
      rules[rule_id] = [lhs, orig_src]
      fout.close
    end

    rules.each_pair do |k, v|
      if !code[v[0]]
        code[v[0]] = ""
      end
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
    meta_tabs = {"t_table_info" => :tabinf, "t_table_schema" => :tabscm, "t_cycle" => :cycle, "t_depends" => :depends, "t_rules" => :rules, "t_provides" => :provides}
    meta = {}
    data = []

    dir = Dir.glob("#{dir}/bud*").first
    ret = DBM.open("#{dir}/the_big_log.dbm")
    ret.each_pair do |k, v|
      key = MessagePack.unpack(k)
      tab = key.shift
      time = key.shift
      # paa: after switch to 1.9, v appears to always be empty
      tup = key[0]
      MessagePack.unpack(v).each {|val| tup << val}
      if meta_tabs[tab]
        raise "non-zero budtime.(tab=#{tab}, time=#{time})  sure this is metadata?" if time != 0 #and strict
        meta[meta_tabs[tab]] ||= []
        meta[meta_tabs[tab]] << tup
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
      meta[:schminf][tab][ts[2]] = ts[1] if ts[2]
    end
    return meta, data
  end

  def mapstr(list)
    list.map{|s| "<th> #{s} </th>"}.join(" ")
  end

  def start_table(dir, tab, time, schema)
    str = "#{dir}/#{tab}_#{time}.html"
    fout = File.new(str, "w")
    fout.puts "<html><title>#{tab} @ #{time}</title>"
    fout.puts "<table border=1>"
    # issue with _snd schemas
    if !schema.nil? and schema[0] == "c_bud_time"
      fout.puts "<tr>"
      if schema[1].length == 2 and schema[1][0].class == Array and schema[1][1].class == Array
        fout.puts mapstr(schema[1][0])
        fout.puts mapstr(schema[1][1])
      elsif schema[1].class == String
        fout.puts mapstr(schema[1..-1])
      else
        fout.puts mapstr(schema[1])
      end
      fout.puts "<tr>"
    end
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
    if row.class < Enumerable
      stream.puts row.map{|c| "<td>#{c.to_s}</td>"}.join(" ")
    else
      # special case for periodics
      stream.puts "<td>#{row.to_s}</td>"
    end
    stream.puts "</tr>"
    stream.close
  end
end
