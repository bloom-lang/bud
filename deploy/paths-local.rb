require 'rubygems'
require 'bud'
# require the meta wrapper
require 'deployer'
require 'localdeploy'

class ShortestPaths
  # include the meta wrapper
  include Bud
  include Deployer
  include LocalDeploy

  state {
    table :link, [:from, :to, :cost]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :mincnt, [:from, :to] => [:mincost, :cnt]
  }

  bootstrap do
    # how many local nodes do we want?
    node_count <= [[2]]
  end

  declare
  def partition
    # same initial data at each node
    initial_data <= node.map do |n|
      [n.uid,
       [[:link, [['a', 'b', 1],
                 ['a', 'b', 4],
                 ['b', 'c', 1],
                 ['c', 'd', 1],
                 ['d', 'e', 1]]
        ]]
      ]
    end
  end

  declare
  def print
    stdio <~ path.map{|p| ["#{@ip}:#{@port}: #{p.inspect}"]}
  end

  declare
  def make_paths
    path <= link.map{|e| [e.from, e.to, e.to, e.cost]}
    path <= join([link, path], [path.from, link.to]).map do |l,p|
      [l.from, p.to, p.from, l.cost+p.cost]
    end
  end

  declare
  def find_shortest
    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    mincnt <= path.group([path.from, path.to], min(path.cost), count)
  end
end

program = ShortestPaths.new(:scoping => true, :ip => "127.0.0.1", :port => 0, :dump => true)
program.run
