REBL stands for "Read Eval Bud Loop" and is Bud's REPL.  Running REBL by typing in "rebl" causes the "rebl>" prompt to appear.  Input is either a collection definition, Bud statement, or REBL command.  REBL commands are prefixed by "/"; all other input is interpreted as Bud code.  If the input begins with either "table", "scratch" or "channel" (the currently-supported collection types), then the input is processed as a collection definition.  Otherwise, the input is processed as a Bud rule.

Rules are not evaluated until a user enters one of the evaluation commands; either "/tick [x]" (tick x times, or once if optional argument x is not specified), or "/run" (runs in the background until a breakpoint is hit, execution stops, or a user inputs "/stop").  Rules may be added or deleted at any time, and collections may similarly be added at any time.

A breakpoint is a Bud rule that has the "breakpoint" scratch on its left-hand-side.  The "/run" command will stop executing at the end of any timestep where a "breakpoint" tuple was derived.  Another invocation of "/run" will continue executing from the beginning of the next timestep.

Let's step through two examples to better understand REBL.  The first example is a centralized all-pairs-all-paths example in a graph, the second example is a distributed ping-pong example.  These examples illustrate the use of the commands, which can be listed from within rebl by typing "/help".


# Shortest Paths Example

Let's start by declaring some collections.  "link" represents directed edges in a graph.  "path" represents all pairs of reachable nodes, with the next hop and cost of the path.

    rebl> table :link, [:from, :to, :cost]
    rebl> table :path, [:from, :to, :next, :cost]

"/lscollections" confirms that the collections are defined.

    rebl> /lscollections
    1: table :link, [:from, :to, :cost]
    2: table :path, [:from, :to, :next, :cost]

We now define some rules to populate "path" based on "link".

    rebl> path <= link {|e| [e.from, e.to, e.to, e.cost]}
    rebl> temp :j <= (link*path).pairs(:to => :from)
    rebl> path <= j { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }

Furthermore, we decide to print out paths to stdout.

    rebl> stdio <~ path.inspected

We provide some initial data to "link".

    rebl> link <= [['a','b',1],['a','b',4],['b','c',1],['c','d',1],['d','e',1]]

Ticking prints out the paths to stdout.

    rebl> /tick
    ["a", "c", "b", 5]
    ["a", "e", "b", 4]
    ["a", "b", "b", 1]
    ["b", "d", "c", 2]
    ["a", "c", "b", 2]
    ["a", "d", "b", 6]
    ["b", "e", "c", 3]
    ["b", "c", "c", 1]
    ["a", "d", "b", 3]
    ["c", "e", "d", 2]
    ["a", "e", "b", 7]
    ["a", "b", "b", 4]
    ["d", "e", "e", 1]
    ["c", "d", "d", 1]

This might be a bit annoying though.  Let's try to remove that rule using "/rmrule".  First, we need to figure out which rule it is, using "/lsrules".

    rebl> /lsrules
    5: link <= [['a','b',1],['a','b',4],['b','c',1],['c','d',1],['d','e',1]]
    1: path <= link {|e| [e.from, e.to, e.to, e.cost]}
    2: temp :j <= (link*path).pairs(:to => :from)
    3: path <= j { |l,p| [l.from, p.to, p.from, l.cost+p.cost] }
    4: stdio <~ path.inspected

Looks like it's rule 4.

    rebl> /rmrule 4

Note how ticking no longer prints out the paths.

    rebl> /tick


# Ping-Pong Example

We begin by starting up two instances of REBL on the same machine in different terminal windows.  Take note of the port number printed when REBL starts up.  For example, we might have:

REBL1 : "Listening on localhost:33483"
REBL2 : "Listening on localhost:48183"

In each REBL, let us now define the following collections and rules:

    rebl> channel :ping, [:@dst, :src]
    rebl> ping <~ ping.map {|p| [p.src, p.dst]}

In REBL1, type in an initial ping to start things off:

    rebl> ping <~ [["localhost:48183", ip_port]]

Note that no messages are exchanged until we either type "/tick [x]" or "/run".  Note that this program will run forever, as pings will contiuously be exchanged.  Let's set a breakpoint so both REBLs break once they've received their first ping.  In both REBLs, type:

    rebl> breakpoint <= ping

Note that the schema of "breakpoint" is unimportant.

Now, let us "/run" both REBLs.  At a leisurely pace, type in "/run" to each REBL.  Hmm, what happened?  Type "/dump ping" on each REBL to see if it got a ping packet:

    rebl> /dump ping

Pings are no longer being exchanged.  Of course, if you want to remove a breakpoint, it is as simple as using "/lsrule" and "/rmrule x", where x is the rule ID shown by "/lsrule".  If you type a "/run" command and want to stop execution, simply type the "/stop" command.