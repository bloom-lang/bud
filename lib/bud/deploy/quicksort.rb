require 'rubygems'
require 'bud'

# Balanced quicksort protocol that uses an expected O(N) time median-finding
# algorithm to select the pivot.
module Quicksort # :nodoc: all
  state do
    scratch :succ, [:elt1, :elt2]
    scratch :pivot, [] => [:elt]
    scratch :gt_pivot, [] => [:list]
    scratch :lt_pivot, [] => [:list]
    scratch :list_to_sort, [] => [:list]
  end

  bloom :quicksort do
    # pick a pivot (median in the list) (expected O(N) time alg)
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

    # stdio <~ pivot.map{|p| [ip_port + " pivot: " + p.elt.to_s]}

    gt_pivot <= ((pivot[[]] and list_to_sort[[]] and
                  [[list_to_sort[[]].list.find_all do |e|
                      e > pivot[[]].elt
                    end]]) or [])
    lt_pivot <= ((pivot[[]] and list_to_sort[[]] and
                  [[list_to_sort[[]].list.find_all do |e|
                      e < pivot[[]].elt
                    end]]) or [])

    # print out successors
    stdio <~ succ.map do |s|
      [ip_port + ": successor: [" + s.elt1.to_s + ", " + s.elt2.to_s + "]"]
    end
  end
end
