require 'rubygems'
require 'bud'
require 'tokenring'

class RingFork
  include Bud
  include TokenRing
  include ForkDeploy

  deploystrap do
    node_count << [10]
  end
end

RingFork.new(:deploy => true).run_fg
