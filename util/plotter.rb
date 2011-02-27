require 'rubygems'
require 'bud'
require 'bud/bud_meta'
require 'bud/graphs'
require 'bud/depanalysis'

# args: N, files_n, classes_m

def process(mods)
  classdef = "class FooBar\ninclude Bud\n" + mods.map{|m| "include #{m}"}.join("\n")  +  "\nend\n FooBar.new"
  puts "classdef #{classdef}"
  d = eval(classdef)

  d.t_rules.each {|s| puts "RULE: #{s.inspect}" }
  d.t_depends.each {|s| puts "DEP: #{s.inspect}" }

  da = d.meta_parser.depanalysis

  puts "MP info src=#{da.source.length}, snk=#{da.sink.length}, under=#{da.underspecified.length}"

  d.meta_parser.depanalysis.source.each do |s|
    puts "SRC: #{s}"
  end

  tabinf = {}
  d.tables.each do |t|
    tabinf[t[0].to_s] = t[1].class.to_s
  end

  gv = GraphGen.new(d.t_stratum, tabinf, d.t_cycle, mods.join("_") + "_viz", -1, 3, ".", true, d.meta_parser.depanalysis)
  gv.process(d.t_depends)
  gv.finish
end

@shreddies = []
@provides = []
@demands = []
@tabinf = {}
@port = 12345
files = []
classes = []

if ARGV.length < 2
  puts "USAGE:\nruby plotter.rb LIST_OF_FILES LIST_OF_MODULES"
  exit
end

modules = []
(0..ARGV.length-1).each do |i|
  begin
    f = File.open(ARGV[i])
    mods = false
  rescue
    mods = true
  end

  unless mods
    puts "DO #{ARGV[i]}"
    eval ( "require \"#{ARGV[i]}\"")
  else
    puts "Work on #{ARGV[i]}"
    #instant(ARGV[i])
    modules << ARGV[i]
    puts "OK"
  end
end

process(modules)

###work
