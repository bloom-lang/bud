require 'rubygems'
require 'bud'
require 'localdeploy-nonmeta'

# XXX: figure out why things are being mass-duplicated
class Mergesort
  include Bud
  include LocalDeploy

  state {
    channel :elt_list, [:@loc] => [:elt_list]
    table :succ, [:elt1, :elt2]
    table :master, [] => [:node]
    table :left_child, [] => [:node]
    table :right_child, [] => [:node]
    table :parent, [] => [:node]
    scratch :gt_pivot, [] => [:list]
    scratch :lt_pivot, [] => [:list]
    scratch :list_to_sort, [] => [:list]
    table :input_list, [] => [:list] # the "input"
    channel :merge_chan, [:@loc, :sender, :elt, :num]
    table :persist_merge_chan, [:sender, :elt, :num]
    table :elt_ord, [:num] => [:elt]
    channel :max_chan, [:@loc, :sender, :max_elt]
    table :persist_max_chan, [:sender] => [:max_elt]
  }

  def initialize(opt)
    super
    @merge_send_counter = -1
    @merge_recv_left_counter = 0
    @merge_recv_right_counter = 0
  end

  def deploystrap
    #input_list << [[2,5,3,6,0,1,8,7,4,9]]
    input_list << [[9,8,7,6]]
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
    initial_data <= ((if node_count[[]] and input_list[[]] and
                          node.map {|n| n}.size == node_count[[]].num
                        node.map do |n|
                          to_send = []
                          eval "to_send = []"
                          # send the master to everybody
                          eval "to_send << [:master, [[me]]]"
                          # send the first node the entire list
                          if n.uid == 0
                            to_send << [:list_to_sort, [[input_list[[]].list]]]
                          else
                            to_send << [:parent, [[node[[(n.uid/2.0).ceil-1]].node]]]
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
  def mergesort_split
    # pop elt_list into list_to_sort
    #stdio <~ list_to_sort.map {|l| [ip_port + " received list: " + l.inspect]}
    list_to_sort <= [elt_list.map {|e| e.elt_list}]

    gt_pivot <= ((list_to_sort[[]] and
                  [[list_to_sort[[]].list[list_to_sort[[]].list.size/2..-1]]]) or [])
    lt_pivot <= ((list_to_sort[[]] and
                  [[list_to_sort[[]].list[0, list_to_sort[[]].list.size/2]]]) or [])

    # upper half of list sent to left child
    elt_list <~ ((gt_pivot[[]] and not gt_pivot[[]].list.empty? and
                  left_child[[]] and sleep(rand) and
                  [[left_child[[]].node, gt_pivot[[]].list]]) or [])
    # lower half of list sent to right child
    elt_list <~ ((lt_pivot[[]] and not lt_pivot[[]].list.empty? and
                  right_child[[]] and sleep(rand) and
                  [[right_child[[]].node, lt_pivot[[]].list]]) or [])
  end

  declare
  def mergesort_merge
    #stdio <~ elt_ord.map {|m| [ip_port + " elt_ord: " + m.inspect]}
    #stdio <~ merge_chan.map {|m| [ip_port + " merge_chan: " + m.inspect]}
    #stdio <~ max_chan.map {|m| [ip_port + " max_chan: " + m.inspect]}

    # 1. node with a single size list_to_sort sends to its parent
    merge_chan <~ ((if list_to_sort[[]] and list_to_sort[[]].list.size == 1
                      [[parent[[]].node, me, list_to_sort[[]].list[0], 0]]
                    end) or [])
    max_chan <~ ((if list_to_sort[[]] and list_to_sort[[]].list.size == 1
                    [[parent[[]].node, me, 0]]
                  end) or [])

    # 1.5 persist merge and max chans
    persist_merge_chan <= merge_chan.map {|m| [m.sender, m.elt, m.num]}
    persist_max_chan <= max_chan.map {|m| [m.sender, m.max_elt]}

    # 1.75 perform the merging
    elt_ord <= ((join([persist_merge_chan, left_child, persist_merge_chan,
                       right_child]).map do |lp, l, rp, r|
                   if lp.sender == l.node and rp.sender == r.node
                     if lp.num == @merge_recv_left_counter and
                         rp.num == @merge_recv_right_counter
                       if lp.elt <= rp.elt
                         @merge_recv_left_counter += 1
                         [@merge_send_counter+=1, lp.elt]
                       else
                         @merge_recv_right_counter += 1
                         [@merge_send_counter+=1, rp.elt]
                       end
                     elsif lp.num == @merge_recv_left_counter and
                         persist_max_chan[[r.node]] and
                         persist_max_chan[[r.node]].max_elt + 1 ==
                         @merge_recv_right_counter
                       @merge_recv_left_counter += 1
                       [@merge_send_counter+=1, lp.elt]
                     elsif persist_max_chan[[l.node]] and
                         persist_max_chan[[l.node]].max_elt + 1 ==
                         @merge_recv_left_counter and
                         rp.num == @merge_recv_right_counter
                       @merge_recv_right_counter +=1
                       [@merge_send_counter+=1, rp.elt]
                     end
                   end
                 end) or [])

    # 2. node that receives two mergedlists merges them and outputs the elements
    # one-at-a-time to parent; sends max to parent when known
    merge_chan <~ ((parent[[]] and
                    elt_ord.map {|e| [parent[[]].node, me, e.elt, e.num]}) or [])
    max_chan <~ ((if left_child[[]] and right_child[[]] and parent[[]] and
                      persist_max_chan[[left_child[[]].node]] and
                      persist_max_chan[[right_child[[]].node]] and
                      persist_max_chan[[left_child[[]].node]].max_elt + 1 ==
                      @merge_recv_left_counter and
                      persist_max_chan[[right_child[[]].node]].max_elt + 1 ==
                      @merge_recv_right_counter
                    [[parent[[]].node, me, @merge_send_counter]]
                  end) or [])

    # 3. the node with no parent does the same, except instead of sending, he
    # outputs to stdout
    succ <= ((if not parent[[]]
                (0..@merge_send_counter).map do |e|
                  if e < @merge_send_counter and elt_ord[[e]] and elt_ord[[e+1]]
                    [elt_ord[[e]].elt, elt_ord[[e+1]].elt]
                  end
                end
              end) or [])

    # master prints out links as soon as they're known
    stdio <~ succ.map do |s|
      [ip_port + ": successor: [" + s.elt1.to_s + ", " + s.elt2.to_s + "]"]
    end

  end

end

program = Mergesort.new(:scoping => true, :ip => "127.0.0.1", :port => 0, :dump_rewrite => true, :deploy => true)
program.run
