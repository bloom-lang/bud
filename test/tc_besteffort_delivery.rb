require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'
require 'tc_delivery'


class TestBED < TestDelivery 
  def test_delivery
    # best effort works when everything else works
    t_delivery("BestEffortDelivery")
  end

end
