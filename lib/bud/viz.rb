require 'rubygems'
require 'syntax/convertors/html'

class Viz 
  def initialize(bud_instance)
    # needs:  class, object_id, options, tables, budtime
    @bud_instance = bud_instance
  end

  def prepare_viz
    #return unless @options[:visualize]
    unless File::directory? "time_pics"
      Dir.mkdir("time_pics")
    end
   
    arr = [@bud_instance.class.to_s, @bud_instance.object_id.to_s]
    arr << @bud_instance.options[:tag] if @bud_instance.options[:tag]
    @time_pics_dir = "time_pics/#{arr.join("_")}"
    create_clean(@time_pics_dir)
    create_clean("plotter_out")
  end

  def visualize(strat, name, rules, depa=nil)
    # collapsed
    gv = GraphGen.new(strat.stratum, @bud_instance.tables, strat.cycle, name, @bud_instance, @time_pics_dir, true, depa)
    gv.process(strat.depends)
    gv.dump(rules)
    gv.finish

    # detail
    gv = GraphGen.new(strat.stratum, @bud_instance.tables, strat.cycle, name, @bud_instance, @time_pics_dir, false, depa)
    gv.process(strat.depends)
    gv.dump(rules)
    gv.finish
  end


  def create_clean(dir)
    if File::directory? dir
      # fix.
      `rm -r #{dir}`
    end
    Dir.mkdir(dir)
  end

  def do_cards
    return unless @bud_instance.options[:visualize]
    cards = {}
    @bud_instance.tables.each do |t|
      #puts "#{@bud_instance.budtime}, #{t[0]}, #{t[1].length}"
      cards[t[0].to_s] = t[1].length
      write_table_contents(t) if @bud_instance.options[:visualize] >= 3
    end
    write_svgs(cards)
    write_html
  end
  
  def write_table_contents(tab)
    fout = File.new("#{@time_pics_dir}/#{tab[0]}_#{@bud_instance.budtime}.html", "w")
    fout.puts "<h1>#{tab[0]} @ #{@bud_instance.budtime}</h1>"
    fout.puts "<table border=1>"
    fout.puts "<tr>" + tab[1].schema.map{|s| "<th> #{s} </th>"}.join(" ") + "<tr>"
    tab[1].each do |row|
      fout.puts "<tr>"
      fout.puts row.map{|c| "<td>#{c.to_s}</td>"}.join(" ")
  
      fout.puts "</tr>"
    end
    fout.puts "</table>"
    fout.close
  end

  def write_svgs(c)
    sts = @bud_instance.meta_parser.strat_state
    return if sts.nil?
    gv = GraphGen.new(sts.stratum, @bud_instance.tables, sts.cycle, "#{@time_pics_dir}/#{@bud_instance.class}_tm_#{@bud_instance.budtime}", @bud_instance, @time_pics_dir, false, @depanalysis, c)
    gv.process(sts.depends)
    gv.finish
  end

  def write_html
    nm = "#{@bud_instance.class}_tm_#{@bud_instance.budtime}"
    prev = "#{@bud_instance.class}_tm_#{@bud_instance.budtime-1}"
    nxt = "#{@bud_instance.class}_tm_#{@bud_instance.budtime+1}"
    fout = File.new("#{@time_pics_dir}/#{nm}.html", "w")
    fout.puts "<center><h1>#{@bud_instance.class} @ #{@bud_instance.budtime}</h1><center>"
    fout.puts "<embed src=\"#{ENV['PWD']}/#{@time_pics_dir}/#{nm}_expanded.svg\" width=\"100%\" height=\"75%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />"
    fout.puts "<hr><h2><a href=\"#{ENV['PWD']}/#{@time_pics_dir}/#{prev}.html\">last</a>"
    fout.puts "<a href=\"#{ENV['PWD']}/#{@time_pics_dir}/#{nxt}.html\">next</a>"
    fout.close
  end

  def dump(shredded_rules)
    return if shredded_rules.nil?

    fout = File.new("plotter_out/style.css", "w")
    fout.puts css
    fout.close

    code = {}
    rules = {}
    convertor = Syntax::Convertors::HTML.for_syntax "ruby"
    shredded_rules.each do |s|
      fout = File.new("plotter_out/#{s[0]}.html", "w+")
      fout.puts header
      fout.puts "<h1>Rule #{s[0]}</h1><br>"

      c = convertor.convert(s[5])
      c.sub!(/^<pre>/, "<pre class=\"code\">\n")
      fout.puts c
      rules[s[0]] = [s[1], s[5]]
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
      fout = File.new("plotter_out/#{k}.html", "w+")
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
