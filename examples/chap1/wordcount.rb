# simple grep
require 'rubygems'
require 'bud'

class WordCount < Bud
  attr_reader :pattern
  
  def initialize(ip, port, pattern)
    super(ip,port)
    @pattern = pattern
  end
  
  def state
    file_reader :text, 'ulysses.txt'
    table :words, ['lineno', 'wordno', 'word']
    table :wc, ['word'], ['cnt']
  end
  
  def declaration
    strata[0] = rules {
      text.each do |t|
        t.text.split.each_with_index {|w,i| words << [t.lineno, i, w]}
      end
    }
    strata[1] = rules {
      wc <= words.group([words.word], count)
    }
  end
end

program = WordCount.new('localhost', ARGV[0], /[Bb]loom/)

program.tick
program.wc.sort{|x,y| x[1] <=> y[1] }.each {|t| puts t.inspect}
