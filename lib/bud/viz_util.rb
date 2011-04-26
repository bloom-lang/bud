require 'rubygems'
require 'bud/graphs'

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
    gv_obj.nodes.each_pair do |k, v|
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
end
