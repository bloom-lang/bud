require 'rubygems'
require 'bud'
# require the meta wrapper
require 'deployer'

class ShortestPaths
  # include the meta wrapper
  include Bud
  include Deployer

  state do
    table :link, [:from, :to, :cost]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :mincnt, [:from, :to] => [:mincost, :cnt]
  end

  bootstrap do
    # which nodes do we want to distribute program to?
    deploy_node <= [[1, "127.0.0.1:54321"]]
    # EDB at each node
    initial_data <= [[1,
                      [[:link, [['a', 'b', 1],
                                ['a', 'b', 4],
                                ['b', 'c', 1],
                                ['c', 'd', 1],
                                ['d', 'e', 1]]
                       ]]
                     ]]
  end

  bloom :print do
    stdio <~ path.map{|p| ["#{@ip}:#{@port}: #{p.inspect}"]}
  end

  bloom :make_paths do
    path <= link.map{|e| [e.from, e.to, e.to, e.cost]}
    path <= join([link, path], [path.from, link.to]).map do |l,p|
      [l.from, p.to, p.from, l.cost+p.cost]
    end
  end

  bloom :find_shortest do
    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    mincnt <= path.group([path.from, path.to], min(path.cost), count)
  end
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1]
program = ShortestPaths.new(:scoping => true, :ip => ip, :port => port)
program.run
