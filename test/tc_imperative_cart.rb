require 'rubygems'
require 'bud'
require 'test/unit'
require 'lib/imperative_cart'

class TestCart < Test::Unit::TestCase
  def advance(p)
    assert_nothing_raised(RuntimeError) {p.tickle}
    sleep 1
  end

  def nudge_read(p, str)
    advance(p)
    assert_equal(1, p.status.length)
    p.status.each do |s|
      assert_equal(str, s.state.to_s) 
    end
  
  end
  
  def test_cart
    program = ImperativeCartServer.new('localhost', 12345)

    program.tick

    program.run_bg
    sleep 1


    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['meat']]]

    nudge_read(program, "meat")
    program.action <+ [['localhost:12345', 'localhost:12345',1234, ['meat', 'beer']]]

    nudge_read(program, "meatbeer")

    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['meat', 'beer', 'diapers']]]
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['beer', 'diapers']]]
    
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['beer', 'beer', 'diapers']]]
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['beer', 'beer', 'beer', 'diapers']]]
    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['beer', 'beer', 'beer', 'beer', 'diapers']]]
    advance(program)

    # I can no longer assert safely what the next value is.
    assert_equal(1, program.status.length)

    program.action <+ [['localhost:12345', 'localhost:12345', 1234, ['beer', 'beer', 'beer', 'diapers']]]


    # I know that 'eventually' the final value will be the last inserted value...
    advance(program)
    advance(program)
    advance(program)
    advance(program)
    advance(program)
    nudge_read(program, "beerbeerbeerdiapers")



  end
end
