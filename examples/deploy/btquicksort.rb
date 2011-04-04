require 'rubygems'
require 'bud'
require 'quicksort'
require 'binarytreepartition'

module BTQuicksort
  include Quicksort
  include BinaryTreePartition

  state do
    channel :elt_list_chan, [:@loc] => [:elt_list]
    channel :succ_chan, [:@loc, :elt1, :elt2]
    table :master, [] => [:node]
    table :input_list, [] => [:list]
  end

  bloom :btquicksort do
    # Send the root the initial list to sort.
    initial_data <= node.map do |n|
      if not tree_parent.has_key? [n.uid]
        [ n.uid, :list_to_sort, [[ input_list[[]].list ]] ]
      end
    end

    # Send every node the master (deployer).
    initial_data <= node.map {|n| [ n.uid, :master, [[ ip_port ]] ]}

    list_to_sort <= [elt_list_chan.map {|e| e.elt_list}]

    # Elements > pivot are sent to left child.
    elt_list_chan <~ (if gt_pivot[[]] and not gt_pivot[[]].list.empty? and
                          left_child[[]] and sleep(rand)
                        [[left_child[[]].node, gt_pivot[[]].list]]
                      else [] end)

    # Elements < pivot are sent to right child.
    elt_list_chan <~ (if lt_pivot[[]] and not lt_pivot[[]].list.empty? and
                          right_child[[]] and sleep(rand)
                        [[right_child[[]].node, lt_pivot[[]].list]]
                      else [] end)

    # Send the known links to the master.
    succ_chan <~ (if master[[]] and lt_pivot[[]] and not
                      lt_pivot[[]].list.empty?
                    [[master[[]].node, lt_pivot[[]].list.max, pivot[[]].elt]]
                  else [] end)
    succ_chan <~ (if master[[]] and gt_pivot[[]] and not
                      gt_pivot[[]].list.empty?
                    [[master[[]].node, pivot[[]].elt, gt_pivot[[]].list.min]]
                  else [] end)

    succ <= succ_chan.map {|s| [s.elt1, s.elt2]}
  end

end
