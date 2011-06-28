Bud: Bloom under development
============================

Welcome to the documentation for *Bud*, a prototype of Bloom under development.

The documents here are organized to be read in any order, but you might like to
try the following:

* [intro.md][intro]: A brief introduction to Bud and Bloom.
* [getstarted.md][getstarted]: A quickstart to teach you basic Bloom
  concepts, the use of `rebl` interactive terminal, and the embedding of Bloom
  code in Ruby via the `Bud` module.
* [operational.md][operational]: An operational view of Bloom, to provide
  a more detailed model of how Bloom code is evaluated by Bud.
* [cheat.md][cheat]: A concise "cheat sheet" to remind you about Bloom syntax.
* [modules.md][modules]: An overview of Bloom's modularity features.
* [ruby\_hooks.md][ruby_hooks]: Bud module methods that allow you to
  interact with the Bud evaluator from other Ruby threads.
* [visualizations.md][visualizations]: Overview of the `budvis` and
  `budplot` tools for visualizing Bloom program analyses.
* [bfs.md][bfs]: A walkthrough of the Bloom distributed filesystem.

[intro]:          /bloom-lang/bud/blob/master/docs/intro.md
[getstarted]:     /bloom-lang/bud/blob/master/docs/getstarted.md
[operational]:    /bloom-lang/bud/blob/master/docs/operational.md
[cheat]:          /bloom-lang/bud/blob/master/docs/cheat.md
[modules]:        /bloom-lang/bud/blob/master/docs/modules.md
[ruby_hooks]:     /bloom-lang/bud/blob/master/docs/ruby_hooks.md
[visualizations]: /bloom-lang/bud/blob/master/docs/visualizations.md
[bfs]:            /bloom-lang/bud/blob/master/docs/bfs.md

In addition, the [bud-sandbox](http://github.com/bloom-lang/bud-sandbox) GitHub
repository contains lots of useful libraries and example programs built using
Bloom.

Finally, the Bud gem ships with RubyDoc on the language constructs and runtime
hooks provided by the Bud module. To see rdoc, run `gem server` from a command
line and open [http://0.0.0.0:8808/](http://0.0.0.0:8808/)
