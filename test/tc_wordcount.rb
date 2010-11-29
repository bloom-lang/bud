# simple grep
require 'rubygems'
require 'bud'
require 'test/unit'
require 'backports'

class WordCount < Bud
  attr_reader :pattern
  
  def initialize(ip, port, pattern)
    super(ip,port)
    @pattern = pattern
  end
  
  def state
    file_reader :txt, '../examples/chap2/ulysses.txt'
#    file_reader :txt, 'shaks12.txt'
    scratch :words, ['lineno', 'wordno'], ['word']
    scratch :wc, ['word'], ['cnt']
    scratch :wc2, ['word'], ['cnt']
    scratch :compare, ['word'], ['cnt', 'cnt2']
  end
  
  declare 
  def program
    words <= txt.flat_map do |t|
      t.text.split.enum_for(:each_with_index).map {|w, i| [t.lineno, i, w]}
    end
    # stratum 1
    wc <= words.group([words.word], count)
    wc2 <= words.reduce({}) do |memo, t|
      memo[t.word] ||= 0
      memo[t.word] += 1
      memo
    end
    compare <= join([wc,wc2], [wc.word, wc2.word]).map { |w, w2| [w.word, w.cnt, w2.cnt] }
  end
end

class TestWC < Test::Unit::TestCase
  def test_wc
    program = WordCount.new('localhost', ARGV[0], /[Bb]loom/)
    assert_nothing_raised { program.tick }
    assert_equal([], program.compare.map {|t| t if t.cnt != t.cnt2}.compact)
    assert_equal(23, program.wc[["yes"]].cnt)
  end
end