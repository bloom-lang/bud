require 'rubygems'
require 'bud'
require 'tokenring'
require 'bud/deploy/forkdeploy'

class RingFork
  include Bud
  include TokenRing
  include ForkDeploy

  deploystrap do
    node_count << [10]
  end
end

RingFork.new(:deploy => true).run_fg
