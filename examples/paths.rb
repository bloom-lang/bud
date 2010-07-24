# simple shortest paths
# note use of program.tick at bottom to run a single timestemp 
# and inspect "shortest" relation
require 'rubygems'
require 'bud'

class ShortestPaths < Bud

  def state
    table :link, ['from', 'to', 'cost']
    table :path, ['from', 'to', 'next', 'cost']
    table :shortest, ['from', 'to'], ['next', 'cost']
    table :minavgs, ['from', 'to'], ['mincost', 'avgcost']
  end
  
  def declaration
    strata[0] = rules {
      link << ['a', 'b', 1]
      link << ['a', 'b', 4]
      link << ['b', 'c', 1]
      link << ['c', 'd', 1]
      link << ['d', 'e', 1]

      path <= link.map{|e| [e.from, e.to, e.to, e.cost]}

      j = join [link, path], [path.from, link.to]
      path <= j.map do |l,p|
        [l.from, p.to, p.from, l.cost+p.cost] # if l.to == p.from
      end
    }

    strata[1] = rules {
      shortest <= path.argmin([path.from, path.to], path.cost)
      minavgs <= path.group([path.from, path.to], min(path.cost), avg(path.cost))
    }
  end
end

program = ShortestPaths.new('localhost', ARGV[0])

program.tick
program.minavgs.each {|t| puts t.inspect}
puts '-----'
program.shortest.each {|t| puts t.inspect}
# 
# program.tick
# program.link << ['e','f',1]
# program.tick
