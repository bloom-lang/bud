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
    table :wc2, ['word'], ['cnt']
    table :compare, ['word'], ['cnt', 'cnt2']
  end
  
  def declaration
    strata[0] = rules {
      text.each do |t|
        t.text.split.each_with_index {|w,i| words << [t.lineno, i, w]}
      end
    }
    strata[1] = rules {
      wc <= words.group([words.word], count)
      wc2 <= words.reduce({}) do |memo, t|
        memo[t.word] ||= 0
        memo[t.word] += 1
        memo
      end
      compare <= join([wc,wc2], [wc.word, wc2.word]).map { |w, w2| [w.word, w.cnt, w2.cnt] }
    }
  end
end

program = WordCount.new('localhost', ARGV[0], /[Bb]loom/)

program.tick
program.compare.each {|t| puts "mismatch: #{t.cnt} != #{t.cnt2}: #{t.inspect}" if t.cnt != t.cnt2}
program.wc.sort{|x,y| x[1] <=> y[1] }.each {|t| puts t.inspect }
