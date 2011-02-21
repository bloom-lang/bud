require 'rubygems'
require 'bud'
# require the meta wrapper
require 'deployer'

class ShortestPaths
  # include the meta wrapper
  include Bud
  include Deployer
  state {
    table :link, [:from, :to, :cost]
    table :path, [:from, :to, :next, :cost]
    table :shortest, [:from, :to] => [:next, :cost]
    table :mincnt, [:from, :to] => [:mincost, :cnt]
  }

  bootstrap do
    # EC2 stuff
    #max_count <= [[1]]
    #min_count <= [[1]]
    #access_key_id <= [["AKIAIBPKK4EH2FWFKOIA"]]
    #secret_access_key <= [["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"]]
    #key_name <= [["wrm"]]
    #ec2_key_location <= [["~/.ssh/ec2"]]

    # which nodes do we want to distribute program to?
    node <= [[1, "127.0.0.1:54321"]]
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

  declare
  def make_paths
    path <= link.map{|e| [e.from, e.to, e.to, e.cost]}
    path <= join([link, path], [path.from, link.to]).map do |l,p|
      (puts "foo") or ($stdout.flush and [l.from, p.to, p.from, l.cost+p.cost])
    end
  end

  declare
  def find_shortest
    shortest <= path.argagg(:min, [path.from, path.to], path.cost)
    mincnt <= path.group([path.from, path.to], min(path.cost), count)
  end
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1]
program = ShortestPaths.new(:scoping => true, :enforce_rewrite => true, :ip => ip, :port => port)
program.run
