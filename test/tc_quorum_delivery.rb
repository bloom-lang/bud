require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestQuorumDelivery < TestDelivery 
  def test_quorum_delivery
    # quorum does basic
    #master = t_delivery("QuorumDelivery")
    #print "master is #{master} class #{master.class}, len #{master.pipe_out.length}\n"
    #assert_equal(0, master.pipe_out.length)

    #sleep 1

    q = q_delivery("QuorumDelivery")

    print "OK then\n"

    q.pipe_out.each do |p|
      print "PO (#{@addy}:#{@budtime} : #{p.inspect}\n"
    end

    assert_equal(1, q.pipe_out.length)

    # reliable is honest about failure
    f_delivery("QuorumDelivery")
  end

end
