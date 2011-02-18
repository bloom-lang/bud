require 'test_common'
require 'bud'

class Semi
    include Bud
    
    state do
      table :stuff
      table :bigstuff, stuff.schema + [:extras]
    end
    
    def bootstrap
      bigstuff << [2, 'crazy', ["check", {:key => 'out'}, ['me', ['and', 'my'], 'nested'], 'ways']]
    end
    
    declare 
    def whynot
      stuff <= bigstuff
    end
end

class TestSemi < Test::Unit::TestCase
  def test_semi
    p = Semi.new
    assert_nothing_raised(RuntimeError) { p.tick }
    # if we kept all the extra stuff, we can "walk" the struct to find things
    assert(p.stuff.first.length > p.stuff.schema.length)
    assert_equal('out', (p.stuff.first[2][1][:key]))
  end
end
