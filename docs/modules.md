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

Within a module

 The BUD Module Import System

## Techniques



