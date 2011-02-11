# simple grep
require 'rubygems'
require 'bud'

class Grep < Bud
  def initialize(pattern)
    super()
    @pattern = pattern
  end

  def state
    file_reader :text, 'ulysses.txt'
    table :matches, [:lineno, :text]
  end

  declare
  def logic
    matches <= text.map{|t| t if t.text =~ @pattern}
  end
end

program = Grep.new(/[Bb]loom/)
program.tick
program.matches.sort.each {|t| puts t.inspect}
