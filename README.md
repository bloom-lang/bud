[![Build Status](https://travis-ci.org/bloom-lang/bud.svg?branch=v0.9.8)](https://travis-ci.org/bloom-lang/bud)
# Bud

This is Bud, a.k.a. "Bloom Under Development".  It is an initial cut at a Bloom
DSL, using Ruby as a setting.

See LICENSE for licensing information.

Language cheatsheet in docs/cheat.md ; see the docs/ directory for other
documentation.

Main deficiencies at this point are:

- No Ruby constraints: Within Bloom programs the full power of Ruby is also
  available, including mutable state. This allows programmers to get outside the
  Bloom framework and lose cleanliness.

- Compatibility: Bud only works with Ruby (MRI) 1.8.7 and 1.9. Bud also has
  experimental support for Ruby 2.0. JRuby and other Ruby implementations are
  currently not supported.

## Installation

To install the latest release:

    % gem install bud

To build and install a new gem from the current development sources:

    % gem build bud.gemspec ; gem install bud*.gem

Note that [GraphViz](http://www.graphviz.org/) must be installed.

Simple example programs can be found in examples. A much larger set of example
programs and libraries can be found in the bud-sandbox repository.

To run the unit tests:

    % gem install minitest      # unless already installed
    % cd test; ruby ts_bud.rb

To run the unit tests and produce a code coverage report:

    % gem install simplecov    # unless already installed
    % cd test; COVERAGE=1 ruby ts_bud.rb

## Optional Dependencies

The bud gem has a handful of mandatory dependencies. It also has one optional
dependency: if you wish to use Bud collections backed by Zookeeper, the
"zookeeper" gem must be installed.
