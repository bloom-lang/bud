require 'rubygems'
require 'bud'
require 'bud/bust/bust'

class BustExample # :nodoc: all
  include Bud
  include Bust

  state do
    table :foo, [:bar, :baz, :qux]
  end

  bloom do
    stdio <~ foo {|t| [t.inspect]}
  end
end

BustExample.new(:bust_port => 8080).run_fg
