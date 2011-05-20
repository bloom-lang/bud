require 'rubygems'
require 'bud'

module TokenRing
  state do
    table :next_node, [] => [:addr]
    channel :next_node_chan, [:@loc] => [:next]
    scratch :send_next_node, [:node, :next]
    table :sent_next_node, [:addr]
    channel :token, [:@loc]
    table :token_persist, [:loc]
  end

  bloom :make_ring do
    # Once a node and its successor have both been computed, send a message to
    # the node with the address of its successor
    send_next_node <= (node * node * node_ready).combos do |n1, n2, nr|
      succ_id = (n1.uid + 1) % node_count[[]].num
      if n2.uid == succ_id and not sent_next_node.has_key? [n1.uid]
        [n1.addr, n2.addr]
      end
    end
    next_node_chan <~ send_next_node
    sent_next_node <+ send_next_node {|n| [n.node]}

    next_node <= next_node_chan {|n| [n.next]}

    # The deployer sends an initial message to the node with ID 0
    token <~ (node_ready * node).rights(:uid => :uid) do |n|
      [n.addr] if (@options[:deploy] and n.uid == 0)
    end
  end

  bloom :pass_token do
    # Persist the token for as long as necessary
    token_persist <= token
    token_persist <- (token_persist * next_node).lefts
    # Pass on the token
    token <~ (token_persist * next_node).rights do |nn|
      [nn.addr]
    end
  end

  bloom :print_token do
    stdio <~ token {["#{@node_id}: Got token! (@ #{ip_port})"]}
  end
end
