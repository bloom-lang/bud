Bud: Bloom under development
============================

Welcome to the documentation for *Bud*, a prototype of Bloom under development.

The documents here are organized to be read in any order, but you might like to
try the following:

* [intro](intro.md): A brief introduction to Bud and Bloom.
* [getstarted](getstarted.md): A quickstart to teach you basic Bloom
  concepts, the use of `rebl` interactive terminal, and the embedding of Bloom
  code in Ruby via the `Bud` module.
* [operational](operational.md): An operational view of Bloom, to provide
  a more detailed model of how Bloom code is evaluated by Bud.
* [cheat](cheat.md): Full documentation of the language constructs in a concise "cheat sheet" style.
* [modules](modules.md): An overview of Bloom's modularity features.
* [ruby_hooks](ruby\_hooks.md): Bud module methods that allow you to
  interact with the Bud evaluator from other Ruby threads.
* [visualizations](visualizations.md): Overview of the `budvis` and
  `budplot` tools for visualizing Bloom program analyses.
* [bfs](bfs.md): A walkthrough of the Bloom distributed filesystem.

In addition, the [bud-sandbox](http://github.com/bloom-lang/bud-sandbox) GitHub
repository contains lots of useful libraries and example programs built using
Bloom.

Finally, the Bud gem ships with RubyDoc on the language constructs and runtime
hooks provided by the Bud module. To see rdoc, run `gem server` from a command
line and open [http://0.0.0.0:8808/](http://0.0.0.0:8808/)
