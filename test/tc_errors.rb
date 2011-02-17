require 'test_common'

class TestErrorHandling < Test::Unit::TestCase
  class EmptyBud
    include Bud
  end

  def test_do_sync_error
    b = EmptyBud.new
    b.run_bg
    3.times {
      assert_raise(ZeroDivisionError) {
        b.sync_do {
          puts 5 / 0
        }
      }
    }

    b.stop_bg
  end
end
