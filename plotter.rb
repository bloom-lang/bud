require 'rubygems' 
require 'bud'
require 'bud/bud_meta'
require 'bud/depanalysis'

# args: N, files_n, classes_m

def work
  # a bit gnarly.  use an empty shell of a bud instance to run 
  # stratification over the composed program...
  ic = Bud.new(:visualize => 3)
  @tabinf.each_pair{|k, v| ic.tables[k] = v } 
  strat = ic.meta_parser.stratify(@shreddies)

  dep = DepAnalysis.new
  strat.depends_tc.each{|d| dep.depends_tc << d; puts "DTC: #{d.inspect}" }

  @provides.each{|p| puts "provide " + p.inspect; dep.providing << p }
  dep.tick
  dep.tick
  dep.tick

  puts "DEP DONE"

  prpr("UNDERSPECIFIED", dep.underspecified)
  prpr("SOURCE", dep.source)
  prpr("SINK", dep.sink)

  strat.depends.each do |d|
    puts "DEP: #{d.inspect}"
  end

  ic.viz.visualize(strat, "outp", @shreddies, dep)
end

def prpr(tab, content)
  puts "#{tab}: "
  content.each {|c| puts "\t#{c.inspect}" }
end

def instant(cls)
  print "try port #{@port}\n\n"
  sleep 1
  d = eval("class FooBar < Bud\ninclude #{cls}\nend\n FooBar.new(:port => #{@port}, :enforce_rewrite => true, :dump => true, :scoping => false)")
  d.meta_parser.shredded_rules.each {|s| @shreddies << s }
  d.provides.each_pair {|k, v| @provides << [k.to_s, v] } 
  d.tables.each_pair {|k, v| @tabinf[k] = v }
  @port = @port + 1
end

@shreddies = []
@provides = []
@demands = []
@tabinf = {}
@port = 12345
files = []
classes = []

if ARGV.length < 2
  print "USAGE:\nruby plotter.rb LIST_OF_FILES LIST_OF_MODULES\n"
  exit
end

(0..ARGV.length-1).each do |i|
  begin
    f = File.open(ARGV[i])
    mods = false
  rescue
    mods = true
  end

  unless mods
    print "DO #{ARGV[i]}\n"
    eval ( "require \"#{ARGV[i]}\"")
  else
    print "Work on #{ARGV[i]}\n"
    instant(ARGV[i])
    print "OK\n"
  end
end

work
