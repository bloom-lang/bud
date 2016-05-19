# Getting Started with Bud #
In this document we'll do a hands-on tour of Bud and its Bloom DSL for Ruby.  We'll start with some examples, and introduce concepts as we go.

## Installation ##
Bud depends on one library that needs to be installed separately:

* [GraphViz](http://www.graphviz.org/Download.php) (2.26.3 recommended)

Once that's done, you know the drill!

    % gem install bud

This installs four things:

* The `Bud` module, to embed Bloom code in Ruby.
* The `rebl` executable: an interactive shell for trying out Bloom.
* The `budplot`, `budvis`, and `budtimelines` executables: graphical tools for visualizing and debugging Bloom programs.

## First Blooms ##

### Hello, Clouds! ###
It seems kind of a silly to do the old "Hello, World" example in a distributed programming language, but no tutorial would be complete without it...

Open up a rebl prompt, and paste in the following:

    stdio <~ [['Hello,'], ['Clouds']]
    /tick
    /q

You should see something like this:

    % rebl
    Welcome to rebl, the interactive Bloom terminal.

    Type: /h for help
          /q to quit

    rebl> stdio <~ [['Hello,'], ['Clouds']]
    rebl> /tick
    Hello,
    Clouds
    rebl> /q

    Rebellion quashed.
    %
    
Let's take this apart:

1. The first line you pasted is a Bloom statement. It says (roughly) "merge the two strings 'Hello,' and 'Clouds' into the standard I/O (terminal) output stream."    Note that rebl doesn't execute that statement immediately!  It just remembers it as part of a set of statements.
2. The second line starts with a slash, meaning it's a command to rebl.  It tells rebl to "tick" the Bloom runtime--that is, to evaluate the set of statements we've typed so far in a single atomic "timestep".  
3. The third line is short for `/quit`.  (By the way, rebl auto-completes its commands with the tab key if you like.)

For fun and illustration, start up rebl again and paste in this minor variation:

    stdio <~ [['Hello,'], ['Clouds'], ['Clouds']]
    /tick
    /tick
    /q
    
What happened?  

First, note that the basic data structures in Bloom are *sets* of objects, which means they have no duplicates, and they have no defined order for their elements.  So you should only see 'Clouds' once on the terminal.  And in fact you may see 'Clouds' and 'Hello,' in either order. That is not a bug, that is a reflection of the disorderly set of values being placed into stdio during a given timestep!  (If you're a fan of duplicates and/or ordering, don't sweat it.  We'll show you how to achieve them.  But remember: Bloom is *disorderly* for a good reason--to reflect the reality of distributed systems execution.  So we're going to try and get you comfortable with being disorderly by default, to protect your ability to write simpler distributed code.  You'll need to use some additional syntax to impose order on in the disorderly context of a distributed system, but it's worth it!)

Second, note that our Bud program's one statement merges the values on its right-hand-side (rhs) into the left-hand-side (lhs) at *every* timestep--every time you say `/tick`. If you ran this program as a server, it would generate an infinite stream of chatter! Generally it doesn't make sense to write server code with constants on the rhs of statements.  We'll see more sensible examples soon.

### Tables and Scratches ###
Before we dive into writing server code, let's try a slightly more involved single-timestep example.  Start up rebl again, and paste in the following:

``` ruby
table :clouds
clouds <= [[1, "Cirrus"], [2, "Cumulus"]]
stdio <~ clouds.inspected
```
    
Now tick your rebl, but don't quit yet.  

    /tick
    
Hopefully the output looks sensible.  A few things to note here:

1. the first line we pasted in is a Bloom *collection declaration*.  It declares the existence of a `table` named `clouds`.  By default, Bloom collections hold \[key, value\] pairs: i.e., arrays with the first field being a unique key, and the second a value. (Given an item `i` in a Bloom collection, you can access those fields as `i.key` and `i.val` respectively, or as `i[0]` and `i[1]`).
2. The second line uses Bloom's `<=` merge operator.  This *instantaneously* merges the contents from the rhs of the statement into the lhs within the same timestep.

3. The third line uses Bloom's `<~` merge operator.  We'll spend more time on the meaning of this operator later; for now just be aware that statements with `stdio` on the lhs *must* use `<~`.  (If you like, try starting over with `<=` instead of `<~` in that statement and see what happens.)
4. the `inspected` method of BudCollections converts arrays of values into strings suitable for printing.  (Again, if you like you can try the program again, leaving out the `.inspected` method.)

Now, let's use rebl's `lsrules` and `rmrule` commands to remove a Bloom statement (a.k.a. "rule") from our program.  Assuming you didn't quit from the last rebl prompt, you can proceed as follows:

    /lsrules
    /rmrule 1
    /lsrules
    
Have a look at the output of each of those rebl commands--they're fairly self-explanatory.  You should see that we deleted the rule that instantaneously merged strings into the `clouds` table.  Now tick your rebl again.

    /tick
    /q
    
You still get output values on this second tick because the clouds table stored its content--even though the statement that populated that table was removed.

In many cases we don't want a collection to retain its contents across ticks.  For those cases, we have `scratch` collections.  Start up a new rebl and try this variant of the previous example:

    scratch :passing_clouds
    passing_clouds <= [[3, "Nimbus"], [2, "Cumulonimbus"]]
    stdio <~ passing_clouds.inspected
    /tick
    /lsrules
    /rmrule 1
    /tick
    /q
    
See how the second tick produced no output this time?  After the first timestep, the passing\_clouds scratch collection did not retain its contents.  And without the first statement to repopulate it during the second timestep, it remained empty.

### Summing Up ###
In these initial examples we learned about a few simple but important things:

* **rebl**: the interactive Bloom terminal and its slash (`/`) commands.
* **Bloom collections**: unordered sets of items, which are set up by collection declarations.  So far we have seen persistent `table` and transient `scratch` collections.  By default, collections are structured as \[key,val\] pairs.
* **Bloom statements**: expressions of the form *lhs op rhs*, where the lhs is a collection and the rhs is either a collection or an array-of-arrays.   
* **Bloom timestep**: an atomic single-machine evaluation of a block of Bloom statements.
* **Bloom merge operators**:
  * The `<=` merge operator for *instantaneously* merging things into a collection.
  * The `<~` merge operator for *asynchronously* merging things into collections outside the control of tick evaluation: e.g. terminal output.
* **stdio**: a built-in Bloom collection that, when placed on the lhs of an asynch merge operator `<~`, prints its contents to stdout.
* **inspected**: a method of Bloom collections that transforms the elements to be suitable for textual display on the terminal.

## Chat, World! ##
Now that we've seen a bit of Bloom, we're ready to write our first interesting service that embeds Bloom code in Ruby.  We'll implement a simple client-server "chat" service. The full code for this program is in the `examples/chat` directory of the Bud distribution.  (Lest there be any confusion at this point, please note that Bloom isn't specifically designed for client/server designs.  Many examples in the [bud-sandbox](http://github.com/bloom-lang/bud-sandbox) repository are more like "peer-to-peer" or "agent-based" designs, which tend to work out as neatly as this one or moreso.)

**Basic idea**: The basic idea of this program is that clients will connect to a chatserver process across the Internet.  When a client first connects to the server, the server will remember its address and nickname.  The server will also accept messages from clients, and relay them to other clients.

Even though we're getting ahead of ourselves, let's have a peek at the Bloom statements that implement the server in `examples/chat/chat_server.rb`:

``` ruby
nodelist <= connect { |c| [c.client, c.nick] }
mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }
```

That's it!  There is one statement for each of the two sentences describing the behavior of the "basic idea" above.  We'll go through these two statements in more detail shortly.  But it's nice to see right away how concisely and naturally a Bloom program can fit our intuitive description of a distributed service.

### The Server Side ###

Now that we've satisfied our need to peek, let's take this a bit more methodically.  First we need declarations for the various Bloom collections we'll be using.  We put the declarations that are common to both client and server into file `examples/chat/chat_protocol.rb`:

``` ruby
module ChatProtocol
  state do
    channel :connect, [:@addr, :client] => [:nick]
    channel :mcast
  end
  
  DEFAULT_ADDR = "localhost:12345"
end
```

This defines a [Ruby mixin module](http://www.ruby-doc.org/docs/ProgrammingRuby/html/tut_modules.html) called `ChatProtocol` that has a couple special Bloom features:

1. It contains a Bloom `state` block, containing collection declarations.  When embedding Bloom in Ruby, all Bloom collection declarations must appear in a `state` block of this sort.
2. This particular state block uses a kind of Bloom collection we have not seen before: a `channel`.  A channel collection is a special kind of scratch used for network communication.  It has a few key features:

  * Unlike the default \[key,val\] structure of scratches and tables, channels default to the structure \[address,val\]: the first field is a destination IP string of the form 'host:port', and the second field is a payload to be delivered to that destination--typically a Ruby array.  (For the record, the default key of a channel collection is the *pair* \[address,val\]).
  * Any Bloom statement with a channel on the lhs must use the async merge (`<~`) operator.  This instructs the runtime to attempt to deliver each rhs item to the address stored therein. In an async merge, each item in the collection on the right will appear in the collection on the lhs *eventually*.  But this will not happen instantaneously, and it might not happen atomically--items in the collection on the rhs may "straggle in" individually over time at the destination.  And if you're unlucky, this may happen after an arbitrarily long delay (possibly never).  The use of `<~` for channels reflects the typical uncertainty of real-world network delivery.  (Don't worry, the Bud sandbox provides libraries to wrap that uncertainty up in convenient ways.)

Given this protocol (and the Ruby constant at the bottom), we're now ready to examine `examples/chat/chat_server.rb` in more detail:

``` ruby
require 'rubygems'
require 'bud'
require_relative 'chat_protocol'

class ChatServer
  include Bud
  include ChatProtocol

  state { table :nodelist }

  bloom do
    nodelist <= connect { |c| [c.client, c.nick] }
    mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }
  end
end

if ARGV.first
  addr = ARGV.first
else
  addr = ChatProtocol::DEFAULT_ADDR
end

ip, port = addr.split(":")
puts "Server address: #{ip}:#{port}"
program = ChatServer.new(:ip => ip, :port => port.to_i)
program.run_fg
```
    
The first few lines get the appropriate Ruby classes and modules loaded via `require`.  We then define the ChatServer class which mixes in the `Bud` module and the ChatProtocol module we looked at above.  Then we have another `state` block that declares one additional collection, the `nodelist` table.  

With those preliminaries aside, we have our first `bloom` block, which is how Bloom statements are embedded into Ruby. Let's revisit the two Bloom statements that make up our server.  

The first is pretty simple: 

``` ruby
nodelist <= connect { |c| [c.client, c.nick] }
```

This says that whenever messages arrive on the channel named "connect", the client address and user-provided nickname should be instantaneously merged into the table "nodelist", which will store them persistently.  Note that nodelist has a \[key/val\] pair structure, so it is suitable for storing pairs of (IP address, nickname).

The next Bloom statement is more complex.  Remember the description in the "basic idea" at the beginning of this section: the server needs to accept inbound chat messages from clients and forward them to other clients.  

``` ruby
mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }
```

The first thing to note is the lhs and operator in this statement.  We are merging items (asynchronously, of course!) into the mcast channel, where they will be sent to their eventual destination.  

The rhs is our first introduction to the `*` operator of Bloom collections, and the `pairs` method after it.  You can think of the `*` operator as "all-pairs": it produces a Bloom collection containing all pairs of mcast and nodelist items.  The `pairs` method iterates through these pairs, passing them through a code block via the block arguments `m` and `n`. Finally, for each such pair the block produces an item containing the `key` attribute of the nodelist item, and the `val` attribute of the mcast item.  This is structured as a proper \[address, val\] entry to be merged back into the mcast channel.  Putting this together, this statement *multicasts inbound payloads on the mcast channel to all nodes in the chat*.

The remaining lines of plain Ruby simply instantiate and run the ChatServer class (which includes the `Bud` module) using an ip and port given on the command line (or the default from ChatProtocol.rb).

#### `*`'s and Clouds ####
You can think of our use of the `*` operator on the rhs of the second statement in a few different ways:

* If you're familiar with event-loop programming, this implements an *event handler* for messages on the mcast channel: whenever an mcast message arrives, this handler performs lookups in the nodelist table to form new messages.  (It is easy to add "filters" to these handlers as arguments to `pairs`.)  The resulting messages are dispatched via the mcast channel accordingly.  This is a very common pattern in Bloom programs: handling channel messages via lookups in a table.

* If you're familiar with SQL databases, the rhs is essentially a query that is run at each timestep, performing a CROSS JOIN of the mcast and nodelist "tables", with the SELECT clause captured by the block. (It is easy to add WHERE clauses to these joins as arguments to `pairs`.)  The resulting "tuples" are "inserted" into the lhs asynchronously (and typical on remote nodes).  This is a general-purpose way to think about the * operator. But as you've already seen, many common use cases for Bloom's * operator don't "feel" like database queries, because one or more of the collections is a scratch that is "driving" the program.

We expect that people doing distributed programming are probably familiar with both of these metaphors, and they're both useful.  It's fairly common to think about rules in the first form, although the second form is actually closer to the underlying semantics of the language (which come from a temporal logic called [Dedalus](http://www.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html)).

### The Client side ###
Given our understanding of the server, the client should be pretty simple.  It needs to send an appropriately-formatted message on the `connect` channel to the server, send/receive messages on the `mcast` channel, and print the messages it receives to the screen.

And here's the code:

``` ruby
require 'rubygems'
require 'bud'
require_relative 'chat_protocol'

class ChatClient
  include Bud
  include ChatProtocol

  def initialize(nick, server, opts={})
    @nick = nick
    @server = server
    super opts
  end

  bootstrap do
    connect <~ [[@server, ip_port, @nick]]
  end

  bloom do
    mcast <~ stdio do |s|
      [@server, [ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]]
    end

    stdio <~ mcast { |m| [pretty_print(m.val)] }
  end

  # format chat messages with timestamp on the right of the screen
  def pretty_print(val)
    str = val[1].to_s + ": " + (val[3].to_s || '')
    pad = "(" + val[2].to_s + ")"
    return str + " "*[66 - str.length,2].max + pad
  end
end

if ARGV.length == 2
  server = ARGV[1]
else
  server = ChatProtocol::DEFAULT_ADDR
end

puts "Server address: #{server}"
program = ChatClient.new(ARGV[0], server, :stdin => $stdin)
program.run_fg
```

The ChatClient class has a typical Ruby `initialize` method that sets up two local instance variables: one for this client's nickname, and another for the 'IP:port' address string for the server.  It then calls the initializer of the Bud superclass passing along a hash of options.

The next block in the class is the first Bloom `bootstrap` block we've seen.  This is a set of Bloom statements that are evaluated only once, just before the first "regular" timestep of the system.  In this case, we bootstrap the client by sending a message to the server on the connect channel, containing the client's address (via the built-in Bud instance method `ip_port`) and chosen nickname.  

After that comes a Bloom block with the name `:chatter`.  It contains two statements: one to take stdio input from the terminal and send it to the server via mcast, and another to receive mcasts and place them on stdio output.  The first statement has the built-in `stdio` scratch on the rhs: this includes any lines of terminal input that arrived since the last timestep.  For each line of terminal input, the `do...end` block formats an `mcast` message destined to the address in the instance variable `@server`, with an array as the payload.  The rhs of the second statement takes `mcast` messages that arrived since the last timestep.  For each message `m`, the `m.val` expression in the block returns the message payload; the call to the Ruby instance method `pretty_print` formats the message so it will look nice on-screen.  These formatted strings are placed (asynchronously, as before) into `stdio` on the left.

The remaining lines are Ruby driver code to instantiate and run the ChatClient class (which includes the `Bud` module) using arguments from the command line.  Note the option `:read_stdin => true` to `ChatClient.new`: this causes the Bud runtime to capture stdin via the built-in `stdio` collection.

### Running the chat ###
You can try out our little chat program on a single machine by issuing each of the following shell commands from the `examples/chat` subdir within a separate window:

    # ruby chatserver.rb

    # ruby chat.rb alice

    # ruby chat.rb bob

    # ruby chat.rb harvey
    
Alternatively you can run the server and clients on separate nodes, specifying the server's IP:port pair on the command-line (consistently).
    
### Summing Up ###
In this section we saw a number of features that we missed in our earlier single-timestep examples in rebl:

* **state blocks**: Embedding of Bloom collection declarations into Ruby.
* **bloom blocks**: Embedding of Bloom statements into Ruby.
* **bootstrap blocks**: for one-time statements to be executed before the first timestep.
* **channel collections**: collection types that enable sending/receiving asynchronous, unreliable messages
* **the * operator and pairs method**: the way to combine items from multiple collections.

# The Big Picture and the Details #
Now that you've seen some working Bloom code, hopefully you're ready to delve deeper.  The [README](README.md) provides links to places you can go for more information.  Have fun and [stay in touch](http://groups.google.com/group/bloom-lang)!
