require 'rubygems'
require 'bud'
require 'test/test_lib'

module TestStuff
  include Anise
  annotator :declare

  def state
    super
    table :pipe_perm, ['dst', 'src', 'pid', 'payload']
  end

  declare
  def memory
    pipe_perm <= pipe_sent.map{|p| p }
  end
end

class BED < Bud
  include BestEffortDelivery
  include TestStuff

  declare
  def memory
    pipe_perm <= pipe_sent.map{|p| p }
  end
end


class RED < Bud
  include ReliableDelivery
  include TestStuff

  declare
  def memory
    pipe_perm <= pipe_sent.map{|p| puts "RED permo" or p }
  end
end


class TestDelivery < TestLib
  def spinup(type, port)
    d = spinup_dist(type, "localhost", port)
    sleep 1
    return d
  end

  def spinup_dist(type, host, port)
    d = nil
    assert_nothing_raised(RuntimeError) { d = eval "#{type}.new(\"#{host}\", #{port}, {'visualize' => true, 'dump' => true, 'enforce_rewrite' => true, 'scoping' => false})" } 
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
