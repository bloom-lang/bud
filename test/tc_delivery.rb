require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'test_lib'


class TestDelivery < TestLib

  def q_delivery(type)
    rd = spinup(type, 12345)
    rd2 = spinup(type, 12346)
    rd3 = spinup(type, 12347)

   assert_nothing_raised(RuntimeError){rd2.run_bg}
   assert_nothing_raised(RuntimeError){rd3.run_bg}

   rd.pipe <+ [['localhost:12346', 'localhost:12345', 1, 'foobar']]
   assert_nothing_raised(RuntimeError){rd.run_bg}

   soft_tick(rd)
   soft_tick(rd2)
   soft_tick(rd3)




   return rd
    

  end
  
  def t_delivery(type)
    print "type is #{type}\n"
    rd = spinup(type, 12345)
    rd2 = spinup(type, 12346)
    assert_nothing_raised(RuntimeError){rd2.run_bg}
    assert_nothing_raised(RuntimeError){rd.run_bg}
    rd.pipe <+ [['localhost:12346', 'localhost:12345', 1, 'foobar']]
    #soft_tick(rd)
    #soft_tick(rd2)

    # received at destination
    assert_equal(1, rd2.pipe_chan.length)
    rd2.pipe_chan.each do |pc|
      assert_equal(1, pc[2])
    end

    # ack'd at sender
    #soft_tick(rd2)
    #assert_equal(1, rd.pipe_out.length)
    #rd.pipe_out.each do |p|
    #assert_equal(1, rd.ack.length)
    #rd.ack.each do |p|
    #  assert_equal(1, p[2])
    #end
    #spinout(rd)
    #spinout(rd2)
    return rd
  end

  def f_delivery(type)
    rd = spinup(type, 12345)
    rd.pipe <+ [['localhost:12346', 'localhost:12345', 1, 'foobar']]
    assert_nothing_raised(RuntimeError){rd.run_bg}

    # NOT ack'd at sender if there was no receiver!
    assert_equal(0, rd.pipe_out.length)
   end 
      
  def spinup(type, port)
    d= spinup_dist(type, "localhost", port)
    #tick

    if d.class == QuorumDelivery
      tick
      d.qdmember << ['localhost:12345']
      d.qdmember << ['localhost:12346']
      d.qdmember << ['localhost:12347']
    end
    return d
  end

  def spinup_dist(type, host, port)
    
    d = eval "#{type}.new(\"#{host}\", #{port})" 
    return d
  end

  def spinout(b)
    (0..2).each do |i|
      assert_nothing_raised(RuntimeError){advance(b)}
    end
  end

end
