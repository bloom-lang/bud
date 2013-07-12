# simple shortest paths
# note use of program.tick at bottom to run a single timestep
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
    path <= link {|l| [l.from, l.to, l.to, l.cost]}
    
    # inductive case: make path of length n+1 by connecting a link to a path of
    # length n
    path <= (link*path).pairs(:to => :from) do |l,p|
      [l.from, p.to, l.to, l.cost+p.cost]
    end
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
program.link <+ [['a', 'b', 1],
                 ['a', 'b', 4],
                 ['b', 'c', 1],
                 ['c', 'd', 1],
                 ['d', 'e', 1]]

program.tick # one timestamp is enough for this simple program
program.shortest.to_a.sort.each {|t| puts t.inspect}

puts "----"

# now lets add an extra link and recompute
program.link <+ [['e', 'f', 1]]
program.tick
program.shortest.to_a.sort.each {|t| puts t.inspect}
