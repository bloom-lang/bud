require 'rubygems' 
require 'bud'
require '../../lib/bud/bud_meta'
require '../../lib/bud/depanalysis'

# args: N, files_n, classes_m

def work
  # a bit gnarly.  use an empty shell of a bud instance to run 
  # stratification over the composed program...
  ic = Bud.new('localhost', 56789)
  @tabinf.each_pair{|k, v| ic.tables[k] = v } 
  strat = ic.stratify(@shreddies)

  dep = DepAnalysis.new("localhost", 23525)

  strat.depends_tc.each{|d| dep.depends_tc << d }
  strat.cycle.each{|d| dep.cycle << d }

  @provides.each{|p| puts "provide " + p.inspect; dep.providing << p }
  @demands.each{|d| puts "demand " + d.inspect; dep.demanding << d }
  dep.tick

  dep.tick
  dep.tick

  dep.pairing.each{|p| puts "PAIRING: #{p.inspect}" }   
  puts "DEP DONE"

  prpr("UNDERSPECIFIED", dep.underspecified)
  prpr("SOURCE", dep.source)
  prpr("SINK", dep.sink)


  ic.visualize(strat, "outp", dep)
end

def prpr(tab, content)
  puts "#{tab}: "
  content.each {|c| puts "\t#{c.inspect}" }
end

def instant(cls)
  print "try port #{@port}\n\n"
  sleep 1
  d = eval("class FooBar < Bud\ninclude #{cls}\nend\n FooBar.new('localhost', #{@port}, {'enforce_rewrite' => true})")
  d.shredded_rules.each {|s| @shreddies << s }
  d.provides.each_pair {|k, v| @provides << [k.to_s, v] } 
  d.demands.each_pair {|k, v| @demands << [k.to_s, v] } 
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

