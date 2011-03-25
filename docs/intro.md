# *Bud*: Ruby <~ Bloom #

Bud is a prototype of the [*Bloom*](http://bloom-lang.org) language for distributed programming, embedded in Ruby.  "Bud" stands for *Bloom under development*.  Bud is currently in alpha; we intend to keep developing Bloom aggressively in the short term, before a firmer beta design.

## Distributed Code in Bloom ##
The goal of Bloom is to make distributed programming far easier than it has been with traditional languages.  The key features of Bloom are:

1. *Disorderly Programming*: Traditional languages like Java and C are based on the [von Neumann model](http://en.wikipedia.org/wiki/Von_Neumann_architecture), where a program counter steps through individual instructions in order. Distributed systems don’t work like that. Much of the pain in traditional distributed programming comes from this mismatch:  programmers are expected to bridge from an ordered programming model into a disorderly reality that executes their code.  Bloom was designed to match--and to exploit--the disorderly reality of distributed systems.   Bloom programmers write code made of unordered collections of statements, and use explicit constructs to impose order when needed.

2. *A Collected Approach to Data Structures*: Taking a cue from successfully-parallelized models like MapReduce and SQL, the standard data structures in Bloom are *disorderly collections*, rather than scalar variables and nested structures like lists, queues and trees. Disorderly collection types reflect the realities of non-deterministic ordering inherent in distributed systems. Bloom provides simple, familiar syntax for manipulating these structures. In Bud, much of this syntax comes straight from Ruby, with a taste of MapReduce and SQL.

3. *CALM Consistency*: Bloom enables powerful compiler analysis techniques based on the [CALM principle](http://db.cs.berkeley.edu/papers/cidr11-bloom.pdf) to reason about the consistency of your distributed code.  The Bud prototype includes program analysis tools that can point out precise *points of order* in your program: lines of code where a coordination library should be plugged in to ensure distributed consistency.

4. *Concise Code*: Bloom is a very high-level language, designed with distributed code in mind.  As a result, Bloom programs tend to be far smaller (often [orders of magnitude](http://boom.cs.berkeley.edu) smaller) than equivalent programs in traditional imperative languages.


## Friends and Family: Come On In ##
Bloom is beginning life as a research project, but our goal is to enable real developers to get real work done.  Faster.  Better.  In a more maintainable and malleable way.

To get to that point, we're offering Bud as a pre-alpha "friends and family" edition of Bloom.  This is definitely the bleeding edge: we're in a rapid  cycle of learning about this new style of programming, and exposing what we learn in new iterations of the language.  If you'd like to jump on the wheel with us and play with Bud, we'd love your feedback--both success stories and constructive criticism.

## Getting Started ##
We're shipping Bud with a [sandbox](http://github.com/bloom-lang/bud-sandbox) of libraries and example applications for distributed systems.  These illustrate the language and how it can be used, and also can serve as mixins for new code you might want to write.  You may be surprised at how short the provided Bud code is, but don't be fooled.

To help newcomers learn the language, we've provided an annotated [cheat sheet](cheat.mdown) to overview the language, a [quick-start tutorial](getstarted.md), and a simple *language reference*.  We also provide instructions and tools for launching Bud code on Amazon's EC2 cloud, and instrumenting what it's doing up there. Like Bud itself, these documents are an early alpha.

We welcome both constructive criticism and (hopefully occasional) smoke-out-your-ears, hair-tearing shouts of frustration.  Please point your feedback cannon at the [Bloom mailing list](http://groups.google.com/group/bloom-lang) on Google Groups.