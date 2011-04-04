require 'rubygems'
require 'bud'
require 'bud/deploy/quicksort'
require 'bud/deploy/binarytreepartition'
require 'bud/deploy/ec2deploy'

# Mixes in quicksort with BinaryTreePartition
class BTQuicksort
  include Bud
  include Quicksort
  include BinaryTreePartition
  include EC2Deploy

  state do
    channel :elt_list_chan, [:@loc] => [:elt_list]
    channel :succ_chan, [:@loc, :elt1, :elt2]
    table :master, [] => [:node]
    table :input_list, [] => [:list] # the "input"
  end

  def deploystrap
#    input_list << [[2,5,3,6,0,1,8,7,4,9]]
    input_list << [[9,1,5]]
    node_count <<
      [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
  end

  bootstrap do
    eval(IO.read('keys.rb'), binding) if File.exists?('keys.rb')
    key_name <= [["wrm"]]
    ec2_key_location <= [["/home/wrm/.ssh/ec2"]]
    ruby_command <= [["ruby btquicksort-ec2.rb"]]
    init_dir <= [["btquicksort-ec2.rb"]]
  end

  bloom :btquicksort do
    # we send the root the initial list to sort
    initial_data <= node.map do |n|
      if not tree_parent.has_key? [n.uid]
        [ n.uid, :list_to_sort, [[ input_list[[]].list ]] ]
      end
    end

    # send every node the master (deployer)
    initial_data <= node.map {|n| [ n.uid, :master, [[ ip_port ]] ]}

    # map elt_list_chan into list_to_sort
    # stdio <~ list_to_sort.map {|l| [ip_port + " received list: " + l.inspect]}
    list_to_sort <= [elt_list_chan.map {|e| e.elt_list}]

    # elements > pivot are sent to left child
    elt_list_chan <~ ((gt_pivot[[]] and not gt_pivot[[]].list.empty? and
                       left_child[[]] and sleep(rand) and
                       [[left_child[[]].node, gt_pivot[[]].list]]) or [])
    # elements < pivot are sent to right child
    elt_list_chan <~ ((lt_pivot[[]] and not lt_pivot[[]].list.empty? and
                       right_child[[]] and sleep(rand) and
                       [[right_child[[]].node, lt_pivot[[]].list]]) or [])

    # send the known links to the master
    succ_chan <~ ((master[[]] and lt_pivot[[]] and not lt_pivot[[]].list.empty? and
                   [[master[[]].node, lt_pivot[[]].list.max, pivot[[]].elt]]) or [])
    succ_chan <~ ((master[[]] and gt_pivot[[]] and not gt_pivot[[]].list.empty? and
                   [[master[[]].node, pivot[[]].elt, gt_pivot[[]].list.min]]) or [])

    succ <= succ_chan.map {|s| [s.elt1, s.elt2]}
  end

end

ip = nil
port = nil
ext_ip = nil
ip, port = ARGV[0].split(':') if ARGV[0]
ext_ip = ARGV[1] if ARGV[1]
BTQuicksort.new(:ip => (ip or "192.168.1.93"),
                :ext_ip => (ext_ip or "99.121.59.132"),
                :port => (port or 54321),
                :deploy => (not ip)).run_fg # XXX hack cuz we don't give an ip for local deploy
