require 'rubygems'
require 'bud'
require 'localdeploy-nonmeta'

# Distributes initial data to nodes 
module BinaryTreePartition
  include BudModule
  include LocalDeploy # XXX

  state do
    scratch :initial_tree_data, [] => [:data]
    scratch :num_levels, [] => [:num]
    table :tree_parent, [:uid] => [:parent_uid]
    table :tree_left_child, [:uid] => [:child_uid]
    table :tree_right_child, [:uid] => [:child_uid]
    table :parent, [] => [:node]
    table :left_child, [] => [:node]
    table :right_child, [] => [:node]
  end

  def deploystrap
    node_count <<
      [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
    super
  end

  bloom :tree_data do
    tree_parent <= node.map do |n|
      if n.uid != 0
        [n.uid, node[[(n.uid/2.0).ceil-1]].uid]
      end
    end

    tree_left_child <= node.map do |n|
      if 2*n.uid + 1 <= node_count[[]].num - 1
        [n.uid, node[[2*n.uid+1]].uid]
      end
    end

    tree_right_child <= node.map do |n|
      if 2*n.uid + 2 <= node_count[[]].num - 1
        [n.uid, node[[2*n.uid+2]].uid]
      end
    end

    initial_data <= join([tree_parent, node],
                         [tree_parent.parent_uid, node.uid]).map do |p, n|
      [ p.uid, :parent, [[ n.node ]] ]
    end

    initial_data <= join([tree_left_child, node],
                         [tree_left_child.child_uid, node.uid]).map do |c, n|
      [ c.uid, :left_child, [[ n.node ]] ]
    end

    initial_data <= join([tree_right_child, node],
                         [tree_right_child.child_uid, node.uid]).map do |c, n|
      [ c.uid, :right_child, [[ n.node ]] ]
    end

  end
end
