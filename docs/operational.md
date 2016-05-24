# An Operational View Of Bloom #
You may ask yourself: well, what does a Bloom program *mean*?  You may ask yourself: How do I read this Bloom code?  ([You may tell yourself, this is not my beautiful house.](http://www.youtube.com/watch?v=I1wg1DNHbNU))

There is a formal answer to these questions about Bloom, but there's also a more more approachable answer.  Briefly, the formal answer is that Bloom's semantics are based in finite model theory, via a temporal logic language called *Dedalus* that is described in [a paper from Berkeley](http://www.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html). 

While that's nice for proving theorems (and writing [program analysis tools](visualizations.md)), many programmers don't find model-theoretic discussions of semantics terribly helpful or even interesting. It's usually easier to think about how the language *works* at some level, so you can reason about how to use it.

That's the goal of this document: to provide a relatively simple, hopefully useful intuition for how Bloom is evaluated.  This is not the only way to evaluate Bloom, but it's the intuitive way to do it, and basically the way that the Bud implementation works (modulo some optimizations).  

## Bloom Timesteps ##
Bloom is designed to be run on multiple machines, with no assumptions about coordinating their behavior or resources.  

Each machine runs an evaluator that works in a loop, as depicted in this figure: 

![Bloom Loop](bloom-loop.png?raw=true)

Each iteration of this loop is a *timestep* for that node; each timestep is associated with a monotonically increasing timestamp (which is accessible via the `budtime` method in Bud). Timesteps and timestamps are not coordinated across nodes; any such coordination has to be programmed in the Bloom language itself.

A Bloom timestep has 3 main phases (from left to right):

1. *setup*: All scratch collections are set to empty.  Network messages and periodic timer events are received from the runtime and placed into their designated `channel` and `periodic` scratches, respectively, to be read in the rhs of statements.  Note that a batch of multiple messages/events may be received at once.
2. *logic*: All Bloom statements for the program are evaluated.  In programs with recursion through instantaneous merges (`<=`), the statements are repeatedly evaluated until a *fixpoint* is reached: i.e., no new lhs items are derived from any rhs.
3. *transition*: Items derived on the lhs of deferred operators (`<+`, `<-`, `<+-`) are placed into/deleted from their corresponding collections, and items derived on the lhs of asynchronous merge (`<~`) are handed off to external code (i.e., the local operating system) for processing.

It is important to understand how the Bloom collection operators fit into these timesteps:

* *Instantaneous* merge (`<=`) occurs within the fixpoint of phase 2.
* *Deferred* operations include merge (`<+`), update (`<+-`), and delete (`<-`), and are handled in phase 3.  Their effects become visible atomically to Bloom statements in phase 2 of the next timestep.
* *Asynchronous* merge (`<~`) is initiated during phase 3, so it cannot affect the current timestep.  When multiple items are on the rhs of an async merge, they may "appear" independently spread across multiple different future local timesteps.


## Atomicity: Timesteps and Deferred Operators ##

The only instantaneous Bloom operator is a merge (`<=`), which can only introduce additional items into a collection--it cannot delete or change existing items.  As a result, all state within a Bloom timestep is *immutable*: once an item is in a collection at timestep *T*, it stays in that collection throughout timestep *T*.  (And forever after, the fact that the item was in that collection at timestep *T* remains true.)

To get atomic state change in Bloom, you exploit the combination of two language features: 

1. the immutability of state in a single timestep, and 
2. the uninterrupted sequencing of consecutive timesteps.  

State "update" is achieved in Bloom via a pair of deferred statements, one positive and one negative, like so:

    buffer <+ [[1, "newval"]]
    buffer <- buffer {|b| b if b.key == 1}

This atomically replaces the entry for key 1 with the value "newval" at the start of the next timestep. As syntax sugar for this common pattern, the deferred update operator can be used:

    buffer <+- [[1, "newval"]]

This update statement removes (from the following timestep) any fact in `buffer` with the key `1`, and inserts (in the following timestep) a fact with the value `[1, "newval"]`. Note that "key" here refers to the key column(s) of the lhs relation: this example assumes `buffer` has a single key column.

Any reasoning about atomicity in Bloom programs is built on this simple foundation.  It's really all you need.  In the bud-sandbox we show how to build more powerful atomicity constructs using it, including things like enforcing [ordering of items across timesteps](https://github.com/bloom-lang/bud-sandbox/tree/master/ordering), and protocols for [agreeing on ordering of distributed updates](https://github.com/bloom-lang/bud-sandbox/tree/master/paxos) across all nodes.

## Recursion in Bloom ##
Because Bloom is data-driven rather than call-stack-driven, recursion may feel a bit unfamiliar at first.

Have a look at the following classic "transitive closure" example, which computes multi-hop paths in a graph based on a collection of one-hop links:

    state do
      table :link, [:from, :to, :cost]
      table :path, [:from, :to,  :cost]
    end

    bloom :make_paths do
      # base case: every link is a path
      path <= link {|e| [e.from, e.to, e.cost]}

      # recurse: path of length n+1 made by a link to a path of length n
      path <= (link*path).pairs(:to => :from) do |l,p|
        [l.from, p.to, l.cost + p.cost]
      end
    end
    
The recursion in the second Bloom statement is easy to see: the lhs and rhs both contain the path collection, so path is defined in terms of itself.

You can think of this being computed by reevaluating the bloom block over and over--within phase 2 of a single timestep--until no more new paths are found.  In each iteration, we find new paths that are one hop longer than the longest paths found previously.  When no new items are found in an iteration, we are at what's called a *fixpoint*, and we can move to phase 3.

Hopefully that description is fairly easy to understand.  You can certainly construct more complicated examples of recursion--just as you can in a traditional language (e.g., simultaneous recursion.)  But understanding this example of simple recursion is probably sufficient for most needs.

## Non-monotonicity and Strata ##

Consider augmenting the previous path-finding program to compute only the "highest-cost" paths between each source and destination, and print them out.  We can do this by adding another statement to the above:

    bloom :print_highest do
      stdio <~ path.argmax([:from, :to], :cost)
    end

The `argmax` expression in the rhs of this statement finds the items in path that have the maximum cost for each `[from, to]` pair.
  
It's interesting to think about how to evaluate this statement.  Consider what happens after a single iteration of the path-finding logic listed above.  We will have 1-hop paths between some pairs.  But there will likely be multi-hop paths between those pairs that cost more.  So it would be premature after a single iteration to put anything out on stdio.  In fact, we can't be sure what should go out to stdio until we have hit a fixpoint with respect to the path collection.  That's because `argmax` is a logically *non-monotonic* operator: as we merge more items into its input collection, it may have to "retract" an output they would previously have produced. 

The Bud runtime takes care of this problem for you under the covers, by breaking your statements in *strata* (layers) via a process called *stratification*.  The basic idea is simple.  The goal is to postpone evaluating non-monotonic operators until fixpoint is reached on their input collections.  Stratification basically breaks up the statements in a Bloom program into layers that are separated by non-monotonic operators, and evaluates the layers in order.  (Note: the singular form of strata is *stratum*).

For your reference, the basic non-monotonic Bloom operators include `group, reduce, argmin, argmax`.  Also, statements that embed Ruby collection methods in their blocks are often non-monotonic--e.g., methods like `all?, empty?, include?, none?` and `size`.

Note that it is possible to write a program in Bloom that is *unstratifiable*: there is no way to separate it into layers like this.  This arises when some collection is recursively defined in terms of itself, and there is a non-monotonic method along the recursive dependency chain.  A simple example of this is as follows:

    glass <= one_item {|t| ['full'] if glass.empty? }

Consider the case where we start out with glass being empty.  Then we know the fact `glass.empty?`, and the bloom statement says that `(glass.empty? => not glass.empty?)` which is equivalent to `(glass.empty? and not glass.empty?)` which is a contradiction.  The Bud runtime detects cycles through non-monotonicity for you automatically when you instantiate your class.
