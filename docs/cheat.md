# Bud Cheat Sheet #

## General Bloom Syntax Rules ##
Bloom programs are unordered sets of statements.<br>
Statements are delimited by semicolons (;) or newlines.<br>
As in Ruby, backslash is used to escape a newline.<br>

## Simple embedding of Bud in a Ruby Class ##
    require 'bud'

    class Foo
      include Bud
        
      state do
        ...
      end
        
      bloom do
        ...
      end
    end
    
## State Declarations ##

A `state` block contains definitions of two kinds of program state:
*collections* and *lattices*. A Bud collection is a *set* of *facts*; each fact
is an array of Ruby values. Note that collections do not contain duplicates
(inserting a duplicate fact into a collection is ignored).

Like a table in a relational database, a subset of the columns in a collection
makeup the collection's _key_. Attempting to insert two facts into a collection
that agree on the key columns (but are not duplicates) results in a runtime
exception.

A lattice represents a value that *grows* over time, where the definition of
"growth" depends on the kind of lattice in question. For example, an `lset`
lattice contains a set of facts that grows over time (similar to a traditional
Bud collection), whereas an `lmax` lattice holds an increasing integer
value. For more information on lattices, see the section below.

### Default Declaration Syntax ###
*BudCollection :name, [keys] => [values]*

### table ###
Contents persist in memory until explicitly deleted.<br>
Default attributes: `[:key] => [:val]`

    table :keyvalue
    table :composite, [:keyfield1, :keyfield2] => [:values]
    table :noDups, [:field1, field2]

### scratch ###
Contents emptied at start of each timestep.<br>
Default attributes: `[:key] => [:val]`

    scratch :stats

### interface ###
Scratch collections, used as connection points between modules.<br>
Default attributes: `[:key] => [:val]`

    interface input, :request
    interface output, :response

### channel ###
Network channel manifested as a scratch collection.<br>
Facts that are inserted into a channel are sent to a remote host; the address of the remote host is specified in an attribute of the channel that is denoted with `@`.<br>
Default attributes: `[:@address, :val] => []`

(Bloom statements with channel on lhs must use async merge (`<~`).)

    channel :msgs
    channel :req_chan, [:cartnum, :storenum, :@server] => [:command, :params]

### loopback ###
A network channel that delivers tuples back to the current Bud instance.<br>
Default attributes: `[:key] => [:val]`

(Bloom statements with loopback on lhs must use async merge (`<~`).)

    loopback :talk_to_self

### periodic ###
System timer manifested as a scratch collection.<br>
System-provided attributes: `[:key] => [:val]`<br>
&nbsp;&nbsp;&nbsp;&nbsp; (`key` is a unique ID, `val` is a Ruby `Time` object.)<br>
State declaration includes interval (in seconds).

(periodic can only be used on rhs of a Bloom statement.)

    periodic :timer, 0.1

Note that because periodics are just a simple wrapper over the system clock, Bud
provides few semantic guarantees about the behavior of periodics. In particular,
periodics execute in a best-effort manner (there is no guarantee of timely
delivery of a periodic tuple), and the system clock value stored in the `val`
field may not be monotonically increasing (e.g., if the system clock is changed
in the midst of Bud execution).

### stdio ###
Built-in scratch collection for performing terminal I/O.<br>
System-provided attributes: `[:line] => []`

Statements with stdio on lhs must use async merge (`<~`).<br>
Using `stdio` on the lhs of an async merge results in writing to the `IO` object specified by the `:stdout` Bud option (`$stdout` by default).<br>
To use `stdio` on rhs, instantiate Bud with `:stdin` option set to an `IO` object (e.g., `$stdin`).<br>

Statements with stdio on lhs must use async merge (`<~`).<br>
Using `stdio` on the lhs of an async merge results in writing to the `IO` object specified by the `:stdout` Bud option (`$stdout` by default).<br>
To use `stdio` on rhs, instantiate Bud with `:stdin` option set to an `IO` object (e.g., `$stdin`).<br>

### halt ###
Built-in scratch collection to be used on the lhs of a rule; permanently halts the Bud instance upon first insertion.  

If the item `[:kill]` is inserted, the Bud OS process (including all Bud instances) is also halted.

### sync ###
Persistent collection mapped to an external storage engine, with synchronous write-flushing each timestep.  
Default attributes: `[:key] => [:val]`.

    sync :s1, :dbm

Currently only [dbm](http://en.wikipedia.org/wiki/Dbm) is supported. Support for tokyo cabinet present in an earlier release has been removed.<br>

### store ###
Persistent collection mapped to an external storage engine, with asynchronous write-flushing.  Supported storage engines: `:zookeeper`.<br>
Default attributes: `[:key] => [:val]`.

Statements with a store on lhs must use async merge (`<~`).<br>

Zookeeper is a special case: it does not take attributes as it trailing arguments.  Instead it requires a `:path` and can optionally take an `:addr` specification (default: `addr => 'localhost:2181'`).

    store :s3, :zookeeper, :path=>"/foo/bar", :addr => 'localhost:2181'

Further info: [Apache Zookeeper](http://hadoop.apache.org/zookeeper/).


## Bloom Statements ##
### Statement Syntax ###
*lhs bloom_op rhs*

Left-hand-side (*lhs*) is a named `BudCollection` object.<br>
Right-hand-side (*rhs*) is a Ruby expression producing a `BudCollection` or `Array` of `Arrays`.<br>
The operator (*bloom_op*) is one of the 5 operators listed below.

### Bloom Operators ###
merges:

* `left <= right` &nbsp;&nbsp;&nbsp;&nbsp; (*instantaneous*)
* `left <+ right` &nbsp;&nbsp;&nbsp;&nbsp; (*deferred*)
* `left <~ right` &nbsp;&nbsp;&nbsp;&nbsp; (*asynchronous*)

delete:

* `left <- right` &nbsp;&nbsp;&nbsp;&nbsp; (*deferred*)

update/upsert:

* `left <+- right` &nbsp;&nbsp;&nbsp; (*deferred*)<br>
deferred insert of items on rhs and deferred deletion of items with matching
keys on lhs. That is, for each fact produced by the rhs, the upsert operator
removes any existing tuples that match on the lhs collection's key columns
before inserting the corresponding rhs fact. Note that both the removal and
insertion operations happen atomically in the next timestep.

### Collection Methods ###
Standard Ruby methods used on a BudCollection `bc`:

implicit map:

    t1 <= bc {|t| [t.col1 + 4, t.col2.chomp]} # formatting/projection
    t2 <= bc {|t| t if t.col == 5}            # selection
    
`flat_map`:

    t3 <= bc.flat_map do |t| # unnest a collection-valued attribute
      bc.col4.map { |sub| [t.col1, t.col2, t.col3, sub] }
    end

`bc.reduce`, `bc.inject`:

    t4 <= bc.reduce({}) do |memo, t|  # example: groupby col1 and count
      memo[t.col1] ||= 0
      memo[t.col1] += 1
      memo
    end

`bc.include?`:

    # This is similar to SQL's NOT IN; note that Bud provides a "notin"
    # collection method that should probably be preferred to this approach.
    t5 <= bc do |t|
        t unless t2.include?([t.col1, t.col2])
    end

## BudCollection-Specific Methods ##
`bc.schema`: returns the schema of `bc` (Hash of key column names => non-key column names; if no non-key columns, just an Array of key column names). Note that for channels, this omits the location specifier (<tt>@</tt>).<br>

`bc.cols`: returns the column names in `bc` as an Array<br>

`bc.key_cols`: returns the key column names in `bc` as an Array<br>

`bc.val_cols`: returns the non-key column names in `bc` as an Array<br>

`bc.keys`: projects `bc` to key columns<br>

`bc.values`: projects `bc` to non-key columns<br>

`chan.payloads`: projects `chan` to non-address columns. Only defined for channels.

    # at sender
    msgs <~ requests {|r| ["127.0.0.1:12345", r]}
    # at receiver
    requests <= msgs.payloads

`bc.inspected`: returns a human-readable version of the contents of `bc`

    stdio <~ bc.inspected

`bc.exists?`: test for non-empty collection.  Can optionally pass in a block.

    stdio <~ [["Wake Up!"] if timer.exists?]
    stdio <~ requests do |r|
      [r.inspect] if msgs.exists?{|m| r.ident == m.ident}
    end
    
`bc.notin(bc2, `*optional hash pairs*`, `*optional ruby block*`)`:<br>
Output the facts in `bc` that do not appear in `bc2`, as follows. First, we form a temporary collection `t` as follows:

  1. Join `bc` and `bc2` according to the specified hash pairs. Hash pairs can
     be fully qualified (`bc.attr1 => bc2.attr2`) or shorthand (`:attr1 =>
     :attr2`).

  2. If a code block is specified, invoke the block on every pair of matching
     tuples in the join result. Any matches for which the block returns `false`
     are removed from `t`.

Finally, we output every tuple of `bc` that does *not* appear in `t`.

    # output items from foo if (a) there is no matching key in bar, or
    # (b) all matching keys in bar have a smaller value
    stdio <~ foo.notin(bar, :key=>:key) {|f, b| f.val <= b.val}

    
## SQL-style grouping/aggregation (and then some) ##

* `bc.group([:col1, :col2], min(:col3))`.  *akin to min(col3) GROUP BY col1,col2*
  * exemplary aggs: `min`, `max`, `bool_and`, `bool_or`, `choose`
  * summary aggs: `sum`, `avg`, `count`
  * structural aggs: `accum`, `accum_pair` *accumulates inputs into a Set; accum_pair takes two inputs and accumulates a Set of pairs (two element arrays)*
* `bc.argmax([:attr1], :attr2)` &nbsp;&nbsp;&nbsp;&nbsp; *returns the bc items per attr1 that have highest attr2*
* `bc.argmin([:attr1], :attr2)`
* `bc.argagg(:exemplary_agg_name, [:attr1], :attr2))`.  *generalizes argmin/max: returns the bc items per attr1 that are chosen by the exemplary
aggregate named*

Note that custom aggregation can be written using `reduce`.

## Collection Combination (Join) ###
To match items across two (or more) collections, use the `*` operator, followed by methods to filter/format the result (`pairs`, `matches`, `combos`, `lefts`, `rights`).

### Methods on Combinations (Joins) ###

`pairs(`*hash pairs*`)`: <br>
Given a `*` expression, form all pairs of items with value matches in the hash-pairs attributes.  Hash pairs can be fully qualified (`coll1.attr1 => coll2.attr2`) or shorthand (`:attr1 => :attr2`).

    # for each inbound msg, find match in a persistent buffer
    result <= (msg * buffer).pairs(:val => :key) {|m, b| [m.address, m.val, b.val] }

`combos(`*hash pairs*`)`: <br>
Alias for `pairs`, more readable for multi-collection `*` expressions.  Must use fully-qualified hash pairs.

    # the following 2 Bloom statements are equivalent to this SQL
    # SELECT r.a, s_tab.b, t.c
    #   FROM r, s_tab, t
    #  WHERE r.x = s_tab.x
    #    AND s_tab.x = t.x;

    # multiple column matches
    out <= (r * s_tab * t).combos(r.x => s_tab.x, s_tab.x => t.x) do |t1, t2, t3|
             [t1.a, t2.b, t3.c]
           end

    # column matching done per pair: this will be very slow
    out <= (r * s_tab * t).combos do |t1, t2, t3|
             [t1.a, t2.b, t3.c] if r.x == s_tab.x and s_tab.x == t.x
           end

`matches`:<br>
Shorthand for `combos` with hash pairs for all attributes with matching names; this is called the "natural join" in SQL.

    # Equivalent to the above statements if x is the only attribute name in common:
    out <= (r * s_tab * t).matches {|t1, t2, t3| [t1.a, t2.b, t3.c]}

`lefts(`*hash pairs*`)`: <br>
Like `pairs`, but implicitly includes a block that projects down to the left item in each pair.

`rights(`*hash pairs*`)`: 
Like `pairs`, but implicitly includes a block that projects down to the right item in each pair.

`outer(`*hash pairs*`)`:<br>
Left Outer Join.  Like `pairs`, but items in the first collection will be produced nil-padded if they have no match in the second collection.

`flatten`:<br>
`flatten` is a bit like SQL's `SELECT *`: it produces a collection of concatenated objects, with a schema that is the concatenation of the schemas in tablelist (with duplicate names disambiguated). Useful for chaining to operators that expect input collections with schemas, e.g., `group`:

    out <= (r * s).matches.flatten.group([:a], max(:b))

## Temp Collections ##
`temp`<br>
Temp collections are scratches defined within a `bloom` block:

    temp :my_scratch1 <= foo

The schema of a temp collection in inherited from the rhs; if the rhs has no
schema, a simple one is manufactured to suit the data found in the rhs at
runtime: `[c0, c1, ...]`.

## Lattices ##

In addition to traditional Bud collections and relational-style statements that
operate over collections, Bud also supports lattices and rules that operate over
lattices. Lattices provide a way to represent values that *grow over time*,
where the definition of "growth" depends on the kind of lattice. The following
built-in lattice types are currently supported:

<table>
  <tr>
    <td><b>Name</b></td>
    <td><b>Description</b></td>
    <td><b>Initial Value</b></td>
    <td><b>Monotone Functions</b></td>
  </tr>

  <tr>
    <td><code>lbool</code></td>
    <td>Threshold test (<code>false</code> => <code>true</code> conditional)</td>
    <td>false</td>
    <td>when_true</td>
  </tr>

  <tr>
    <td><code>lmax</code></td>
    <td>Increasing numeric value</td>
    <td>-&infin;</td>
    <td>gt(n), gt_eq(n), +(n), -(n)</td>
  </tr>

  <tr>
    <td><code>lmin</code></td>
    <td>Decreasing numeric value</td>
    <td>+&infin;</td>
    <td>lt(n), lt_eq(n), +(n), -(n)</td>
  </tr>

  <tr>
    <td><code>lset</code></td>
    <td>Growing set of values</td>
    <td>empty set</td>
    <td>contains?, eqjoin, filter, intersect, product, project, size</td>
  </tr>

  <tr>
    <td><code>lpset</code></td>
    <td>Growing set of non-negative numeric values</td>
    <td>empty set</td>
    <td>contains?, eqjoin, filter, intersect, product, project, size, sum</td>
  </tr>

  <tr>
    <td><code>lbag</code></td>
    <td>Growing multiset of values</td>
    <td>empty multiset</td>
    <td>contains?, multiplicity, intersect, product, project, size</td>
  </tr>

  <tr>
    <td><code>lmap</code></td>
    <td>Map from keys to lattice values</td>
    <td>empty map</td>
    <td>at, intersect, key?, key_set, project, size</td>
  </tr>
</table>

Lattices can be declared in `state` blocks in a similar manner to traditional
Bud collections. Similarly, Bloom rules can invoke functions on lattice
values. A simple Bloom program that uses lattices to compute a quorum vote is as
follows:

```ruby
QUORUM_SIZE = 5
RESULT_ADDR = "example.org"

class QuorumVote
  include Bud

  state do
    channel :vote_chn, [:@addr, :voter_id]
    channel :result_chn, [:@addr]
    lset    :votes
    lmax    :vote_cnt
    lbool   :vote_done
  end

  bloom do
    votes      <= vote_chn {|v| v.voter_id}
    vote_cnt   <= votes.size
    got_quorum <= vote_cnt.gt_eq(QUORUM_SIZE)
    result_chn <~ got_quorum.when_true { [RESULT_ADDR] }
  end
end
```

For more information on lattice support in Bloom, see this [recent paper](http://db.cs.berkeley.edu/papers/socc12-blooml.pdf).

## Bud Modules ##
A Bud module combines state (collections) and logic (Bloom rules). Using modules allows your program to be decomposed into a collection of smaller units.

Defining a Bud module is identical to defining a Ruby module, except that the module can use the `bloom`, `bootstrap`, and `state` blocks described above.

There are two ways to use a module *B* in another Bloom module *A*:

  1. `include B`: This "inlines" the definitions (state and logic) from *B* into
     *A*. Hence, collections defined in *B* can be accessed from *A* (via the
     same syntax as *A*'s own collections). In fact, since Ruby is
     dynamically-typed, Bloom statements in *B* can access collections
     in *A*!

  2. `import B => :b`: The `import` statement provides a more structured way to
     access another module. Module *A* can now access state defined in *B* by
     using the qualifier `b`. *A* can also import two different copies of *B*,
     and give them local names `b1` and `b2`; these copies will be independent
     (facts inserted into a collection defined in `b1` won't also be inserted
     into `b2`'s copy of the collection).

In practice, a Bloom program is often composed of a collection of modules (which
may themselves include or import sub-modules) and one "top-level class" that
includes/imports those modules as well as the `Bud` module. An instance of this
top-level class represents an instance of the Bud interpreter; it is on this
top-level class that the `run_fg` method should be invoked, for example.

Note that to enable the Bloom DSL for a collection of Ruby code, it is
sufficient to include the `Bud` module *once* in the top-level class. That is,
you should *not* include `Bud` in every Bloom module that you write.

## Skeleton of a Bud Module ##

    require 'rubygems'
    require 'bud'

    module YourModule
      import SubModule => :sub_m

      state do
        ...
      end

      bootstrap do
        ...
      end

      bloom :some_stmts do
        ...
      end

      bloom :more_stmts do
        ...
      end
    end

    class TopLevelClass
      include Bud
      include YourModule
    end
