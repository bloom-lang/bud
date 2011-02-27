require 'rubygems'
require 'bud'
require 'localdeploy-nonmeta'

class Quicksort
  include Bud
  include LocalDeploy

  state {
    channel :elt_list, [:@loc] => [:elt_list]
    channel :succ, [:@loc, :elt1, :elt2]
    table :master, [] => [:node]
    table :left_child, [] => [:node]
    table :right_child, [] => [:node]
    scratch :pivot, [] => [:elt]
    scratch :gt_pivot, [] => [:list]
    scratch :lt_pivot, [] => [:list]
    scratch :list_to_sort, [] => [:list]
    table :input_list, [] => [:list] # the "input"
  }

  def deploystrap
    input_list << [[2,5,3,6,0,1,8,7,4,9]]
    #input_list << [[9,8,7,6]]
    # building a full tree; could get same parallelism with half the nodes, but
    # don't feel like complicating the code that much
    # 2^ceil(log_2(x)) levels (# of nodes is 2*|levels| - 1)
    node_count << [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
    # node i in the list has its children at 2i+1 and 2i+2
    super
  end


  declare
  def partition
    # XXX: by the way, this is a huge hack because i'm too lazy to write out
    # the count aggregate; ".size" just seems more natural to me.  Hopefully
    # we'll support this syntax eventually
    #stdio <~ [[Process.pid.to_s + " node size is " + node.map {|n| n}.size.to_s]]
    #stdio <~ ((node_count[[]] and
    #             [["node count is: " + node_count[[]].num.to_s]]) or [])

    initial_data <= ((if node_count[[]] and input_list[[]] and
                          node.map {|n| n}.size == node_count[[]].num
                        node.map do |n|
                          to_send = []
                          # send the master to everybody
                          eval "to_send << [:master, [[me]]]"
                          # send the first node the entire list
                          if n.uid == 0
                            to_send << [:list_to_sort, [[input_list[[]].list]]]
                          end
                          # if the node has children, send those
                          if 2*n.uid + 1 <= node_count[[]].num - 1
                            to_send << [:left_child, [[node[[2*n.uid+1]].node]]]
                          end
                          if 2*n.uid + 2 <= node_count[[]].num - 1
                            to_send << [:right_child, [[node[[2*n.uid+2]].node]]]
                          end
                          [n.uid, to_send]
                        end
                      end) or [])
  end

  declare
  def quicksort
    # pop elt_list into list_to_sort
    #stdio <~ list_to_sort.map {|l| [ip_port + " received list: " + l.inspect]}
    list_to_sort <= [elt_list.map {|e| e.elt_list}]

    # pick a pivot (median in the list) (expected O(N) time alg)
    # using median as pivot cuz i'm not sure how to build a "circular tree"
    # fixed topology with O(N) nodes that can deal with arbitrary imbalances
    pivot <= ((list_to_sort[[]] and
               if idempotent [[:pivot, list_to_sort[[]].list]]
                 ((def find_median(list, k)
                     return list[0] if list.size == 1
                     rand_elt = list[rand(list.size)]
                     smaller = list.find_all {|e| e < rand_elt}
                     return rand_elt if smaller.size == k-1
                     if smaller.size < k - 1
                       find_median(list.find_all  {|e| e > rand_elt},
                                   k - 1 - smaller.size)
                     else
                       find_median(smaller, k)
                     end
                   end) or
                  [[find_median(list_to_sort[[]].list,
                                list_to_sort[[]].list.size/2)]])
               end
               ) or [])

    #stdio <~ pivot.map{|p| [ip_port + " pivot: " + p.elt.to_s]}

    gt_pivot <= ((pivot[[]] and list_to_sort[[]] and
                  [[list_to_sort[[]].list.find_all do |e|
                      e > pivot[[]].elt
                    end]]) or [])
    lt_pivot <= ((pivot[[]] and list_to_sort[[]] and
                  [[list_to_sort[[]].list.find_all do |e|
                      e < pivot[[]].elt
                    end]]) or [])

    # elements > pivot are sent to left child
    elt_list <~ ((gt_pivot[[]] and not gt_pivot[[]].list.empty? and
                  left_child[[]] and sleep(rand) and
                  [[left_child[[]].node, gt_pivot[[]].list]]) or [])
    # elements < pivot are sent to right child
    elt_list <~ ((lt_pivot[[]] and not lt_pivot[[]].list.empty? and
                  right_child[[]] and sleep(rand) and
                  [[right_child[[]].node, lt_pivot[[]].list]]) or [])

    # send the links we know to the master
    succ <~ ((master[[]] and lt_pivot[[]] and not lt_pivot[[]].list.empty? and
              [[master[[]].node, lt_pivot[[]].list.max, pivot[[]].elt]]) or [])
    succ <~ ((master[[]] and gt_pivot[[]] and not gt_pivot[[]].list.empty? and
              [[master[[]].node, pivot[[]].elt, gt_pivot[[]].list.min]]) or [])

    # master prints out the links as it gets them
    stdio <~ succ.map do |s|
      [ip_port + ": successor: [" + s.elt1.to_s + ", " + s.elt2.to_s + "]"]
    end
  end

end

program = Quicksort.new(:scoping => true, :ip => "127.0.0.1", :port => 0, :dump_rewrite => true, :deploy => true)
program.run
