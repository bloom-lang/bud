# simple shortest paths
# note use of program.tick at bottom to run a single timestemp 
# and inspect relations
require 'rubygems'
require 'bud'

class ShortestPaths
  include Bud

  state do
    table :link, [:from, :to, :cost]
    table :path, [:from, :to, :nxt, :cost]
    table :shortest, [:from, :to] => [:nxt, :cost]
  end

  # recursive rules to define all paths from links
  bloom :make_paths do
    # base case: every link is a path
    path <= link {|e| [e.from, e.to, e.to, e.cost]}
    
    # inductive case: make path of length n+1 by connecting a link to a path of length n
    temp :j <= (link*path).pairs(:to => :from)
    path <= j { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }
  end

  # find the shortest path between each connected pair of nodes
  bloom :find_shortest do
    shortest <= path.argmin([path.from, path.to], path.cost)
  end
end

# compute shortest paths.
program = ShortestPaths.new

# populate our little example.  we put two links between 'a' and 'b'
# to see whether our shortest-paths code does the right thing.
program.link <= [['a', 'b', 1],
                 ['a', 'b', 4],
                 ['b', 'c', 1],
                 ['c', 'd', 1],
                 ['d', 'e', 1]]

program.tick # one timestamp is enough for this simple program
program.shortest.sort.each {|t| puts t.inspect}

puts "----"

# now lets add an extra link and recompute
program.link << ['e', 'f', 1]
program.tick
program.shortest.sort.each {|t| puts t.inspect}
