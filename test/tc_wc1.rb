# simple word count
require 'test_common'
require 'backports'

class WordCount1
  include Bud

  attr_reader :pattern

  def initialize(pattern)
    super()
    @pattern = pattern
  end

  state do
    file_reader :txt, 'text/ulysses.txt'
    # file_reader :txt, 'shaks12.txt'
    scratch :wc, [:word] => [:cnt]
  end

  bloom do
    wc <= txt.flat_map do |t|
            t.text.split.enum_for(:each_with_index).map {|w, i| [t.lineno, i, w]}
          end.rename([:lineno, :wordno, :word]).group([:word], count)
  end
end

class TestWC1 < Test::Unit::TestCase
  def test_wc1
    program = WordCount1.new(/[Bb]loom/)
    assert_nothing_raised { program.tick }
    assert_equal(23, program.wc[["yes"]].cnt)
  end
end
