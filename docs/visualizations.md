# Visualizations

Bud programs compile naturally to dataflows, and dataflows have a natural
graphical representation.  Viewing the dataflow graph of a program can be useful
to developers at various stages of program design, implementation and debugging.

Bud ships with two visualization utilities, __budplot__ and __budvis__.  Both
use _GraphViz_ to draw a directed graph representing the program state and
logic.  __budplot__ provides a static analysis of the program, identifying
sources and sinks of the dataflow, unconnected components, and "points of order"
that correspond to logically nonmonotonic path edges. __budvis__ is an offline
debugging tool that analyses the trace of a (local) Bud execution and provides
an interactive representation of runtime state over time.

## Using budplot

__budplot__ is a visual static analysis tool that aids in design and early
implementation.  The most common uses of __budplot__ are:

1. Visual sanity checking: does the dataflow look like I expected it to look?
2. Ensuring that a particular set of mixins is fully specified: e.g., did I forget to include a concrete implementation of a protocol required by other modules?
   The Bloom module system, abstract interfaces and concrete implementations are described in more detail in [modules.md](modules.md).
3. Identifying dead code
4. Experimenting with different module compositions
5. Identifying and iteratively refining a program's "points of order"

To run __budplot__, specify a list of Ruby input files, followed by a list of
Bud modules to be "mixed in" in the visualization.

    $ budplot
    Usage: budplot LIST_OF_FILES LIST_OF_MODULES

For example:

    $ budplot kvs/kvs.rb ReplicatedKVS
    Warning: underspecified dataflow: ["my_id", true]
    Warning: underspecified dataflow: ["add_member", true]
    Warning: underspecified dataflow: ["send_mcast", true]
    Warning: underspecified dataflow: ["mcast_done", false]
    fn is ReplicatedKVS_viz_collapsed.svg
    $ open bud_doc/index.html

`ReplicatedKVS` includes the `MulticastProtocol` and `MembershipProtocol`
protocols, but does not specify which implementation of these abstractions to
use.  The program is underspecified, and this is represented in the resulting
graph (`ReplicatedKVS_viz_collapsed.svg`) by a node labeled "??" in the
dataflow.

    $ budplot kvs/kvs.rb ReplicatedKVS BestEffortMulticast StaticMembership
    fn is ReplicatedKVS_BestEffortMulticast_StaticMembership_viz_collapsed.svg
    $ open bud_doc/index.html

## Using budvis

To enable tracing, we need to set `:trace => true` in the `Bud` constructor, and
optionally provide a `:tag` to differentiate between traces by a human-readable
name (rather than by `object_id`).  I modified the unit test `test/DBM_kvs.rb` as
follows:

    - v = BestEffortReplicatedKVS.new(@opts.merge(:port => 12345))
    - v2 = BestEffortReplicatedKVS.new(@opts.merge(:port => 12346))

    + v = BestEffortReplicatedKVS.new(@opts.merge(:port => 12345, :tag => 'dist_primary', :trace => true))
    + v2 = BestEffortReplicatedKVS.new(@opts.merge(:port => 12346, :tag => 'dist_backup', :trace => true))


Then I ran the unit test:

    $ ruby test/tc_kvs.rb 
    Loaded suite test/tc_kvs
    Started
    .Created directory: DBM_BestEffortReplicatedKVS_dist_primary_2160259460_
    Created directory: DBM_BestEffortReplicatedKVS_dist_primary_2160259460_/bud_
    Created directory: DBM_BestEffortReplicatedKVS_dist_backup_2159579740_
    Created directory: DBM_BestEffortReplicatedKVS_dist_backup_2159579740_/bud_
    ..
    Finished in 4.366793 seconds.
    
    3 tests, 14 assertions, 0 failures, 0 errors

Then I ran the visualization utility:

    $ budvis DBM_BestEffortReplicatedKVS_dist_primary_2160259460_/

And finally opened the (chronological) first output file:

    $ open -a /Applications/Google\ Chrome.app/ DBM_BestEffortReplicatedKVS_dist_primary_2160259460_/tm_0_expanded.svg
