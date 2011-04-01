# Code Structuring and Reuse in BUD

## Language Support

### Ruby Mixins

The basic unit of reuse in BUD is the mixin functionality provided by Ruby itself.  BUD code is structured into modules, each of which may have its own __state__ and__bootstrap__ block and any number of __bloom__ blocks (described below).  A module or class may mix in a BUD module via Ruby's _include_ statement.  _include_ causes the specified module's code to be expanded into the local scope.

### Bloom ``Baskets'' (Bouquets?)

While the order and grouping of BUD rules have no semantic significance, rules can be grouped and tagged within a single module using __bloom__ blocks or baskets.  Baskets serve two purposes:
 
 1. Improving readability by grouping related or dependent rules together.
 2. Supporting name-based overriding.

(1) is self-explanatory.  (2) is one of several extensibility mechanisms provided by BUD.  If a Module B includes a module A which contains a basket X, B may supply a basket X and in so doing replaces the set of rules defined by A with its own set.  For example:

    module Hello
      bloom :hi do
        stdio <~ 
      end
    end


### The BUD Module Import System

For simple programs, composing modules via _include_ is often sufficient.  But the flat namespace provided by mixins can make it difficult or impossible to support certain types of reuse.  Consider a module Q that provides a queue-like functionality via an input interface _enqueue_ and an output interface _dequeue_, each with a single attribute (payload).  A later module may wish to employ two queues (say, to implement a scheduler).  But it cannot include Q twice!  It would be necessary to rewrite Q's interfaces so as to support multiple ``users.'' 

In addition to _include_, BUD supports the _import_ keyword, which instantiates a BUD module under a namespace alias.  For example:

    module UserCode
      import Q => :q1
      import Q => :q2

      bloom do
        # insert into the first queue
        q1.enqueue <= [....]
      end
    end
## Techniques



