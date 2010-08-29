require 'rubygems'
require 'bud'
require 'test/unit'
require 'examples/cart'

class TestCart < Test::Unit::TestCase
  def advance(p)
    assert_nothing_raised(RuntimeError) {p.tickle}
    sleep 1
  end
  
  def test_cart
    program = BasicCartServer.new('localhost', 12345)

    program.tick

    program.run_bg
    sleep 1


    program.action <+ [['localhost:12345', 'localhost:12345', 1234, 'meat', 'A', 123]]
    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'beer', 'A', 124]]
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, 'diapers', 'A', 125]]
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, 'meat', 'D', 126]]

    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'beer', 'A', 127]]
    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'beer', 'A', 128]]
    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'beer', 'A', 129]]
    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'beer', 'D', 130]]

    program.checkout <+ [['localhost:12345', 'localhost:12345',1234]]

    advance(program)


    program.action <+ [['localhost:12345', 'localhost:12345',1234, 'papers', 'A', 131]]

    advance(program)
    advance(program)

    program.status.each do |a|
      if a.item == "beer"
        assert_equal(3, a.cnt)
      elsif a.item == "diapers"
        assert_equal(1, a.cnt)
      else
        assert_error("incorrect item #{a.item} in cart")
      end
    end
  end
end
