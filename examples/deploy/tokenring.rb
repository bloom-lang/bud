require 'rubygems'
require 'bud'

module TokenRing
  state do
    table :next_node, [] => [:node]
    channel :token, [:@loc]
    table :token_persist, [:loc]
  end

  bloom :make_ring do
    initial_data <= node do |n|
      # Calculate the successor node (splice deployer into the list)
      succ_id = (n.uid + 1) % (node_count[[]].num + 1)
      succ_node = (succ_id == node_count[[]].num) ? [ip_port] :
        [node[[succ_id]].node]
      [ n.uid, :next_node, [ succ_node ] ]
    end

    # Initial data for deployer (not part of "node" set)
    next_node <= node do
      [node[[0]].node] if @options[:deploy]
    end
    token <~ node do
      [node[[0]].node] if @options [:deploy]
    end
  end

  bloom :pass_token do
    # Persist the token for as long as necessary
    token_persist <= token
    token_persist <- (token_persist * next_node).lefts {|t| [t.loc]}
    # Pass on the token
    token <~ (token_persist * next_node).combos {[next_node[[]].node]}
#    stdio <~ token {["#{ip_port}: Got token!"]}
  end
end
