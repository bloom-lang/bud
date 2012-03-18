# *Bud*: Ruby <~ Bloom #

Bud is a prototype of the [*Bloom*](http://bloom-lang.org) language for distributed programming, embedded as a DSL in Ruby.  "Bud" stands for *Bloom under development*.  The current release is the initial alpha, targeted at "friends and family" who would like to engage at an early stage in the language design.

## Distributed Code in Bloom ##
The goal of Bloom is to make distributed programming far easier than it has been with traditional languages.  The key features of Bloom are:

1. *Disorderly Programming*: Traditional languages like Java and C are based on the [von Neumann model](http://en.wikipedia.org/wiki/Von_Neumann_architecture), where a program counter steps through individual instructions in order. Distributed systems don’t work like that. Much of the pain in traditional distributed programming comes from this mismatch:  programmers are expected to bridge from an ordered programming model into a disorderly reality that executes their code.  Bloom was designed to match--and to exploit--the disorderly reality of distributed systems.   Bloom programmers write code made of unordered collections of statements, and use explicit constructs to impose order when needed.

2. *A Collected Approach to Data Structures*: Taking a cue from successfully-parallelized models like MapReduce and SQL, the standard data structures in Bloom are *disorderly collections*, rather than scalar variables and nested structures like lists, queues and trees. Disorderly collection types reflect the realities of non-deterministic ordering inherent in distributed systems. Bloom provides simple, familiar syntax for manipulating these structures. In Bud, much of this syntax comes straight from Ruby, with a taste of MapReduce and SQL.

3. *CALM Consistency*: Bloom enables powerful compiler analysis techniques based on the [CALM principle](http://db.cs.berkeley.edu/papers/cidr11-bloom.pdf) to reason about the consistency of your distributed code.  The Bud prototype includes program analysis tools that can point out precise *points of order* in your program: lines of code where a coordination library should be plugged in to ensure distributed consistency.

4. *Concise Code*: Bloom is a very high-level language, designed with distributed code in mind.  As a result, Bloom programs tend to be far smaller (often [orders of magnitude](http://boom.cs.berkeley.edu) smaller) than equivalent programs in traditional imperative languages.

## Alpha Goals and Limitations ##

We had three main goals in preparing this release.  The first was to flesh out the shape of the Bloom language: initial syntax and semantics, and the "feel" of embedding it as a DSL.  The second goal was to build tools for reasoning about Bloom programs: both automatic program analyses, and tools for surfacing those analyses to developers.

The third goal was to start a feedback loop with developers interested in the potential of the ideas behind the language.  We are optimistic that the principles underlying Bloom can make distributed programming radically simpler.  But we realize that those ideas only matter if programmers can adopt them naturally.  We intend Bud to be the beginning of an iterative design partnership with developers who see value in betting early on these ideas, and shaping the design of the language.  

In developing this alpha release, we explicitly set aside some issues that we intend to revisit in future.  The first limitation is performance: Bud alpha is not intended to excel in single-node performance in terms of either latency, throughput or scale.  We do expect major improvements on all these fronts in future releases: many of the known performance problems have known solutions that we've implemented in prior systems.  The second main limitation involves integration issues embedding Bloom as a DSL in Ruby.  In the spectrum from flexibility to purity, we leaned decidedly toward flexibility.  The barriers between Ruby and Bloom code are very fluid in the alpha, and we do relatively little to prevent programmers from ad-hoc mixtures of the two.  Aggressive use of Ruby within Bloom statements is likely to do something *interesting*, but not necessarily predictable or desirable.  This is an area where we expect to learn more from experience, and make some more refined decisions for the beta release.

### Friends and Family: Come On In ###
Although our team has many years of development experience, Bud is still open-source academic software built by a small group of researchers.

This alpha is targeted at "friends and family", and at developers who'd like to become same.  This is definitely the bleeding edge: we're in a rapid  cycle of learning about this new style of programming, and exposing what we learn in new iterations of the language.  If you'd like to jump on the wheel with us and play with Bud, we'd love your feedback--both success stories and constructive criticism.

## Getting Started ##
We're shipping Bud with a [sandbox](http://github.com/bloom-lang/bud-sandbox) of libraries and example applications for distributed systems.  These illustrate the language and how it can be used, and also can serve as mixins for new code you might want to write.  You may be surprised at how short the provided Bud code is, but don't be fooled.

To get you started with Bud, we've provided a [quick-start tutorial](getstarted.md) and a number of other docs you can find linked from the [README](README.md).

We welcome both constructive criticism and (hopefully occasional) smoke-out-your-ears, hair-tearing shouts of frustration.  Please point your feedback cannon at the [Bloom mailing list](http://groups.google.com/group/bloom-lang).

Happy Blooming!
