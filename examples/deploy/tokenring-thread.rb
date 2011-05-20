require 'rubygems'
require 'bud'
require 'tokenring'

class RingThread
  include Bud
  include TokenRing
  include ThreadDeploy

  deploystrap do
    node_count << [10]
  end
end

RingThread.new(:deploy => true).run_fg
