require 'rubygems'
require 'bud'
require 'tokenring'
require 'bud/deploy/localdeploy'

class RingLocal
  include Bud
  include TokenRing
  include LocalDeploy

  deploystrap do
    node_count << [10]
  end

end

RingLocal.new(:deploy => true).run_fg
