require 'rubygems'
require 'bud'
require 'btquicksort'
require 'bud/deploy/localdeploy'

# Mixes in quicksort with BinaryTreePartition
class BTQuicksortLocal
  include Bud
  include BTQuicksort
  include LocalDeploy

  deploystrap do
    input_list << [[2,5,3,6,0,1,8,7,4,9]]
    node_count <<
      [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
  end

end

BTQuicksortLocal.new(:ip => "127.0.0.1", :deploy => true).run_fg
