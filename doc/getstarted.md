# Getting Started with Bud #
In this document we'll do a hands-on tour of Bud and its Bloom DSL for Ruby.  We'll start with some examples, and introduce concepts as we go.

## Installation ##
You know the drill!

    % gem install bud

This installs four things:

* The `Bud` module, to embed Bloom code in Ruby.
* The `rebl` executable: an interactive shell for Bloom statements.
* The `budplot` and `budvis` executables: graphical tools for visualizing and debugging Bloom programs.

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

1. The first line is a Bloom statement. It says "merge the two strings 'Hello,' and 'Clouds' into the standard I/O (terminal) output stream."    Note that rebl doesn't execute that statement immediately!  It just remembers it as part of a block of statements.
2. The second line starts with a slash, meaning it's a command to rebl -- it tells rebl to "tick" the Bloom runtime, by evaluating the block of statements so far in a single atomic "timestep".  The Bud runtime evaluates our program's one statement by collecting up all the items to merge into stdio, then printing them to the terminal.
3. The third line is short for `/quit`.  (rebl auto-completes its commands with the tab key if you like.)

Just for fun, start up rebl again and paste in this little variation:

    stdio <~ [['Hello,'], ['Clouds'], ['Clouds']]
    /tick
    /tick
    
What happened?  First, note that Bloom's merge operation suppresses duplicate values!  The basic data structures in Bloom are *sets* of objects, which means they have no duplicates, and they have no defined order for their elements.  (In fact you may have seen `Clouds\nHello,` in one of the examples above ... which is also correct!)  If you're a fan of duplicates and/or ordering, don't sweat it.  We'll show you how to achieve those.  But remember: Bloom is *disorderly* for a good reason -- to reflect the reality of distributed systems execution!  So we're going to make you use a bit more syntax to impose order.

Second, note that our Bud program's one statement merges the values on its right-hand-side (rhs) into the left-hand-side (lhs) at *every* timestep -- every time you say `/tick`. If you ran this program as a server, it would generate an infinite stream of chatter! Clearly it doesn't make sense to write server code with constants on the rhs of statements.  We'll see more sensible examples soon.

### Tables and Scratches ###
Before we dive into writing server code, let's try a slightly more involved single-timestep example.  Start up rebl again, and paste in the following:

    table :clouds
    clouds <= [[1, "Cirrus"], [2, "Cumulus"]]
    stdio <~ clouds.inspected
    
Now tick your rebl, but don't quit yet.  

    /tick
    
Hopefully the output looks sensible.  A few things to note here:

1. the first line is a Bloom *collection declaration*.  It declares the existence of a `table` named `clouds`.  By default, Bloom collections hold key/value pairs: arrays with the first field being a unique key, and the second a value. 
2. The second line uses Bloom's `<=` merge operator.  This merges the contents of the statements rhs into the lhs *instantaneously*, within the timestep of a single tick.
3. The third line uses Bloom's `<~` merge operator.  We'll spend more time on the meaning of this operator later; for now just be aware that statements with `stdio` on the lhs *must* use `<~`.  (If you like, try replacing `<~` with `<=` in that statement.)
4. the `inspected` method of BudCollections converts arrays of values into strings suitable for printing.  (If you like, try the program again, leaving out the `.inspected` method.)

Now, let's use rebl's `lsrules` and `rmrule` commands to remove a Bloom statement (a.k.a. "rule") from our program.  Assuming you didn't quit from the last rebl prompt, you can proceed as follows:

    /lsrules
    /rmrule 1
    /lsrules
    
You should see that we deleted the rule that instantaneously merged strings into the `clouds` table.  Now tick your rebl again.

    /tick
    /q
    
You see output values on this second tick because the clouds table stored its content--even though the statement that populated it is gone.

In many cases we don't want a collection to retain its contents across ticks.  For those cases, we have `scratch` collections.  Start up a new rebl and try this variant of the previous example:

    scratch :passing_clouds
    passing_clouds <= [[3, "Nimbus"], [2, "Cumulonimbus"]]
    stdio <~ passing_clouds.inspected
    /tick
    /lsrules
    /rmrule 1
    /tick
    /q
    
See how the second tick produced no output?  After the first timestep the "scratch" collection forgot its contents.  And without the first statement to repopulate it during the second timestep, it remained empty.

### Summing Up ###
In these brief examples we learned about a few simple but important things:

* Using the rebl interactive terminal and its `/` commands.
* Bloom collections: unordered sets that can be declared as persistent `table` or transient `scratch` collections.  By default, collections are key/value pairs.
* The basic syntax of Bloom statements: *lhs op rhs*, where the lhs is a collection and the rhs is either a collection or an array-of-arrays.
* The concept of a Bloom `timestep` -- an atomic single-machine evaluation of a block of statements.
* Using Bloom's built-in `stdio` collection and `<~` merge operator to put collections into terminal output.
* The `<=` merge operator for *instantaneously* merging things into a collection.
* The `inspected` method of collections, which prepares them for display on the terminal.

## Chat, World! ##
Now that we've seen a bit of Bloom, we're ready to write our first interesting program that embeds Bloom code in Ruby.  We'll implement a simple client-server "chat" program. All the code for this program is in the `examples/chat` directory of the Bud distribution.  Note that Bloom isn't specifically designed for client/server designs--many examples in the [bud-sandbox](http://github.com/bloom-lang/bud-sandbox) repository are symmetric ("peer-to-peer", "agent-based") designs, and they often work out as neatly as this one or moreso.

**Basic idea**: The basic idea of this program is that clients will connect to a chatserver process across the Internet.  When a client first connects to the server, the server will remember its address and nickname.  The server will also accept messages from clients, and relay them to other clients.

Even though we're getting ahead of ourselves, have a peek at the Bloom statements that implement the server in `examples/chat/chatserver.rb`:

    nodelist <= signup.payloads
    mcast <~ join([mcast, nodelist]) do |m,n| 
      [n.key, m.val] unless n.key == m.val[0]
    end

That's it!  There is one statement for each of the two sentences describing the behavior of the "basic idea" above.  We'll go through them in more detail shortly, but it's nice to see right away how concise a Bloom program can be, and how naturally it fits the way we tend to describe distributed systems.

### The Server Side ###

Now that we've satisfied our need to peek, let's take this a bit more methodically.  First we need declarations for the various Bloom collections we'll be using.  Look at the file `examples/chat/chat_protocol.rb`:

    module ChatProtocol
      state do
        channel :mcast
        channel :connect
      end
    end

This defines a [Ruby mixin module](http://www.ruby-doc.org/docs/ProgrammingRuby/html/tut_modules.html) called `ChatProtocol` that has a couple special Bloom features:

1. It contains a Bloom `state` block.  When embedding Bloom in Ruby, all Bloom collection declarations must appear in a `state` block of this sort.
2. This state block uses a kind of Bloom collection we have not seen before: a `channel`.  A `channel` collection is a special kind of scratch used for network communication.  It has a few key features:

  * Unlike the default "key/value" structure of scratches and tables, channels default to the structure "address/payload": the first field is a destination IP string of the form 'host:port', and the second field is a payload to be delivered to that destination -- typically a Ruby array.
  * Any Bloom statement with a channel on the lhs must use the `<~` merge operator.  This operator is an *asynchronous* merge. Each item in the collection on the right will appear in the collection on the lhs *at the address in that item's address field*.  But this will not happen instantaneously (of course).  It will not even happen atomically  -- items in the collection on the rhs may ``straggle in'' individually over time at the destination.  And if you're unlucky, it may happen after an arbitrarily long delay (i.e. never).  The use of `<~` for channels reflects the typical uncertainty of real-world network delivery.  (Don't worry, Bloom provides libraries to wrap that uncertainty up in the usual convenient ways.)

Given this protocol, we're now ready to examine `examples/chat/chatserver.rb` in more detail:

    # simple chat
    require 'rubygems'
    require 'bud'
    require 'chat_protocol'

    class ChatServer
      include Bud
      include ChatProtocol

      state { table :nodelist }

      bloom :server_logic do
        nodelist <= connect.payloads
        mcast <~ join([mcast, nodelist]) do |m,n| 
          [n.key, m.val] unless n.key == m.val[0]
        end
      end
    end

    ip, port = ARGV[0].split(':')
    program = ChatServer.new({:ip => ip, :port => port.to_i})
    program.run
    
The first few lines get the appropriate Ruby classes and modules loaded via `require`, and then define the ChatServer class which mixes in the `Bud` module and the `ChatProtocol` module we looked at above.  Then we have another `state` block that declares one additional collection, the `nodelist` table.  By now we know what that means.  

Finally we have our first `bloom` block!  This is the way we embed Bloom statements into Ruby.  By giving the bloom block a name (`:server_logic` in this case) we can override that block of statements in subclass of `ChatServer` later if desired.  (The name is optional though.)

Now its time to examine the two Bloom statements that make up our server.  The first is quite intuitive: 

     nodelist <= connect.payloads

This says that whenever messages arrive on the channel named `connect`, their payloads (i.e. their second field) should be instantaneously merged into the table nodelist, which will store them persistently.  (Note that nodelist has a "key/value" pair structure, so we expect the payloads will have that structure as well.)

The next Bloom statement is more complex.  Remember the description in the "basic idea" at the beginning of this section: the server needs to accept inbound chat messages from clients and forward them to other clients.  

    mcast <~ join([mcast, nodelist]) do |m,n| 
      [n.key, m.val] unless n.key == m.val[0]
    end
    
The first thing to note is the lhs and operator: we are merging items (asynchronously, of course!) into the mcast channel, where they will be sent to their eventual destination.  The rhs requires us to understand the Bloom `join` method.  The argument to join is a list of collections -- in this case, the `mcast` channel and `nodelist` table.  What join does is form all pairs of items from those two collections, and pass each pair into the Ruby `do...end` block via the block arguments `m` and `n`.  

So what is happening in that block?  For each pair of an inbound mcast message and a node, we create an item to merge into mcast in the outbound direction.  The item's address is `n.key`: the address of a node that has "connected" at some point via the previous rule.  The item's payload is `m.val`: the payload of the inbound mcast message.  In short, this statement *forwards a copy of each mcast payload to each address in the nodelist.*

The remaining two lines are Ruby driver code to instantiate and run the ChatServer class (which includes the `Bud` module) running at an ip and port given on the command line.

### The client side ###
Given our understanding of the server, the client should be pretty simple.  It needs to send an appropriately-formatted message on the connect channel to the server, send/receive messages on the mcast channel, and print the messages it receives to the screen.

And here's the code:

    # simple chat
    require 'rubygems'
    require 'bud'
    require 'chat_protocol'

    class ChatClient
      include Bud
      include ChatProtocol

      def initialize(nick, server, opts)
        @nick = nick
        @server = server
        super opts
      end

      # send connection request to server on startup
      bootstrap do
        connect <~ [[@server, [ip_port, @nick]]]
      end

      bloom :chatter do
        # send mcast requests to server
        mcast <~ stdio do |s|
          [@server, [ip_port, @nick, Time.new.strftime("%I:%M.%S"), s.line]]
        end
        # pretty-print mcast msgs from server on terminal
        stdio <~ mcast do |m|
          [left_right_align(m.val[1].to_s + ": " \
                            + (m.val[3].to_s || ''),
                            "(" + m.val[2].to_s + ")")]
        end
      end

      # format chat messages with timestamp on the right of the screen
      def left_right_align(x, y)
        return x + " "*[66 - x.length,2].max + y
      end
    end

    program = ChatClient.new(ARGV[0], ARGV[1], {:read_stdin => true})
    program.run

The ChatClient class has a typical Ruby `initialize` method that sets up some local instance variables for this client's nickname and the IP:port for the server.  It then calls the initializer of the Bud superclass with a hash of options.

The next block in the class is our first Bloom `bootstrap` block.  This is a set of Bloom statements that are only evaluated once, just before the first "regular" timestep of the system.  In this case, we bootstrap the client by sending a properly-formatted message to the server registering the client's address (via the built-in Bud instance method `ip_port`) and chosen nickname.  

After that comes a normal Bloom block, with the name `:chatter`.  It contains two statements: one to send stdio input out via mcast, and another to receive mcasts and place them on stdio output.  The first statement has the built-in `stdio` scratch on the rhs: this includes any lines of terminal input that arrived since the last timestep.  For each line of terminal input, the `do...end` block formats an `mcast` message destined to the address in the instance variable `@server`, with an array as the payload.  The rhs of the second statement takes `mcast` messages that arrived since the last timestep.  For each message `m`, the `m.val` expression in the block returns the message payload; the call to the Ruby instance method `left_right_align` formats the message so it will look nice on-screen.  These formatted strings are placed (asynchronously, as before) into `stdio` on the left.

The remaining two lines are Ruby driver code to instantiate and run the ChatClient class (which includes the `Bud` module) using arguments from the command line.

# The Big Picture: Bloom Concepts #
*now we're ready to present/review Bloom semantics more carefully, starting with the single-node timestep model, the operator types, and the collection types.*