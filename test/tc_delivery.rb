require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'test_lib'


class TestDelivery < TestLib

  def add_members(b, *hosts)
    hosts.each do |h|
      assert_nothing_raised(RuntimeError) { b.member << [h] }
    end
  end

  def spinup(type, port)
    d = spinup_dist(type, "localhost", port)
    sleep 1
    return d
  end

  def spinup_dist(type, host, port)
    d = nil
    assert_nothing_raised(RuntimeError) { d = eval "#{type}.new(\"#{host}\", #{port})" } 
    assert_nothing_raised(RuntimeError) { d.run_bg }     
    return d
  end

  def spinout(b)
    (0..2).each do |i|
      assert_nothing_raised(RuntimeError){advance(b)}
    end
  end

end
