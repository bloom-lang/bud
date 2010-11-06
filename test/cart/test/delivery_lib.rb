require 'rubygems'
require 'bud'
require 'test/test_lib'


class TestDelivery < TestLib
  def spinup(type, port)
    d = spinup_dist(type, "localhost", port)
    sleep 1
    return d
  end

  def spinup_dist(type, host, port)
    d = nil
    assert_nothing_raised(RuntimeError) { d = eval "#{type}.new(\"#{host}\", #{port}, {'visualize' => true, 'dump' => true})" } 
    #assert_nothing_raised(RuntimeError) { d = eval "#{type}.new(\"#{host}\", #{port}, {'visualize' => false})" } 
    assert_nothing_raised(RuntimeError) { d.run_bg }     
    return d
  end

  def spinout(b)
    (0..2).each do |i|
      assert_nothing_raised(RuntimeError){advance(b)}
    end
  end

end
