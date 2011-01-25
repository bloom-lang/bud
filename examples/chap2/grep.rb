# simple grep
require 'rubygems'
require 'bud'

class Grep < Bud
  def initialize(ip, port, pattern)
    super(ip, port)
    @pattern = pattern
  end

  def state
    file_reader :text, 'ulysses.txt'
    table :matches, ['lineno', 'text']
  end

  declare
  def logic
    matches <= text.map{|t| t if t.text =~ @pattern}
  end
end

program = Grep.new('localhost', ARGV[0], /[Bb]loom/)
program.tick
program.matches.sort.each {|t| puts t.inspect}
