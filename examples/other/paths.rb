# simple shortest paths
# note use of program.tick at bottom to run a single timestemp 
# and inspect relations
require 'rubygems'
require 'bud'

class ShortestPaths
  include Bud

  state do
    table :link, [:from, :to, :cost]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :mincnt, [:from, :to] => [:mincost, :cnt]
  end

  bootstrap do
    link <= [['a', 'b', 1],
             ['a', 'b', 4],
             ['b', 'c', 1],
             ['c', 'd', 1],
             ['d', 'e', 1]]
  end

  declare
  def make_paths
    path <= link.map{|e| [e.from, e.to, e.to, e.cost]}
    j = join [link, path], [path.from, link.to]
    path <= j.map do |l,p|
      [l.from, p.to, p.from, l.cost+p.cost]
    end
  end

  declare
  def find_shortest
    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    mincnt <= path.group([path.from, path.to], min(path.cost), count)
  end

  def print_paths
    mincnt.each {|t| puts t.inspect}
    puts '-----'
    shortest.each {|t| puts t.inspect}
  end
end

program = ShortestPaths.new
program.tick
program.print_paths

program.link << ['e', 'f', 1]
program.tick
