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
  end
  
  def declaration
    strata[0] = rules {
      link << ['a', 'b', 1]
      link << ['a', 'b', 3]
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
      path.reduce(shortest) do |memo,p| 
        if memo[[p.from,p.to]].nil? or (memo[[p.from,p.to]].cost > p.cost) then 
          memo[[p.from,p.to]] = [p.next, p.cost]
        end
        memo
      end
    }
  end
end

program = ShortestPaths.new('localhost', ARGV[0])

program.tick
program.shortest.each {|t| puts t.inspect}
# 
# program.tick
# program.link << ['e','f',1]
# program.tick
