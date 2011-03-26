# Visualizations

BUD programs compile naturally to dataflows, and dataflows have a natural graphical representation.  Plotting a program as a graph can be useful to developers at various stages of program design, implementation and debugging.  BUD predicate dependency graphs (or PDGs) are described on [[PDGs]].

BUD ships with two visualization utilities, __plotter__ and __visualizer__.  Both use _GraphViz_ to draw a directed graph representing the program state and logic.  __plotter__ provides a static analysis of the program, identifying sources and sinks of the dataflow, unconnected components, and points of order corresponding to logically nonmonotonic path edges. __visualizer__ is an offline debugging tool that analyses the trace of a (local) BUD execution and provides an interactive representation of runtime state over time.

## The Plotter 

[[https://github.com/bloom-lang/bud/blob/master/util/budplot]]

The __plotter__ is a visual static analysis tool that aids in design and early implementation.  The most common uses of the __plotter__ are:

1. Visual sanity checking: does the dataflow look like I expected it to look?
2. Ensuring that a particular set of mixins is fully specified: e.g., did I forget to include a concrete implementation of a protocol required by other modules?
    * JMH: What's a "concrete implementation" really?  Will readers follow?
3. Identifying dead code sections
4. Experimenting with different module compositions
5. Identifying and iteratively refining a program's points of order
    * JMH: you need a link to a definition of points of order somewhere

    $ ruby budplot 
    USAGE:
    ruby budplot LIST_OF_FILES LIST_OF_MODULES

As its usage message indicates, __plotter__ expects a list of ruby input files, followed by a list of BUD modules to mix in.

    $ ruby budplot kvs/kvs.rb ReplicatedKVS
    Warning: underspecified dataflow: ["my_id", true]
    Warning: underspecified dataflow: ["add_member", true]
    Warning: underspecified dataflow: ["send_mcast", true]
    Warning: underspecified dataflow: ["mcast_done", false]
    fn is ReplicatedKVS_viz_collapsed.svg
    $ open -a /Applications/Google\ Chrome.app/ ReplicatedKVS_viz_collapsed.svg

__ReplicatedKVS__ includes the __MulticastProtocol__ and __MembershipProtocol__ protocols, but does not specify which implementation of these abstractions to use.  The program is underspecified, and this is represented in the resulting graph (ReplicatedKVS_viz_collapsed.svg) by a node labeled "??" in the dataflow.

```
$ ruby budplot kvs/kvs.rb ReplicatedKVS BestEffortMulticast StaticMembership
fn is ReplicatedKVS_BestEffortMulticast_StaticMembership_viz_collapsed.svg
$ open -a /Applications/Google\ Chrome.app/ ReplicatedKVS_BestEffortMulticast_StaticMembership_viz_collapsed.svg
```

* JMH: I assume the below will be cleaned up?

draft:

* (not technically a static analysis, but it quacks like one)
* experiment with different compositions
* visualize global distributed program 
* find and manipulation points of order
* spot underspecified / (statically) dead code
* peek at metadata?

## The Visualizer

[[https://github.com/bloom-lang/bud/blob/master/util/budvis]]

To enable tracing, we need to set __:trace => true__ in the __BUD__ constructor, and optionally provide a __:tag__ to differentiate between traces by a human-readable name (rather than by object_id).  I modified the unit test `test/tc_kvs.rb` as follows:

```
- v = BestEffortReplicatedKVS.new(@opts.merge(:port => 12345))
- v2 = BestEffortReplicatedKVS.new(@opts.merge(:port => 12346))

+ v = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_primary', :port => 12345, :trace => true))
+ v2 = BestEffortReplicatedKVS.new(@opts.merge(:tag => 'dist_backup', :port => 12346, :trace => true))

```

Then I ran the unit test:

```
$ ruby test/tc_kvs.rb 
Loaded suite test/tc_kvs
Started
.Created directory: TC_BestEffortReplicatedKVS_dist_primary_2160259460_
Created directory: TC_BestEffortReplicatedKVS_dist_primary_2160259460_/bud_
Created directory: TC_BestEffortReplicatedKVS_dist_backup_2159579740_
Created directory: TC_BestEffortReplicatedKVS_dist_backup_2159579740_/bud_
..
Finished in 4.366793 seconds.

3 tests, 14 assertions, 0 failures, 0 errors
```

Then I ran the visualization utility:

```
$ ruby budvis TC_BestEffortReplicatedKVS_dist_primary_2160259460_/
```

And finally opened the (chronological) first output file:

```
$ open -a /Applications/Google\ Chrome.app/ TC_BestEffortReplicatedKVS_dist_primary_2160259460_/tm_0_expanded.svg
```
