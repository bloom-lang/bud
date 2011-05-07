require 'rubygems'
require 'bud'
require 'tokenring'

class RingAft
  include Bud
  include TokenRing
  include AftDeploy

  deploystrap do
    node_count << [10]
  end
end

RingAft.new(:deploy => true).run_fg
