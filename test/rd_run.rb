require 'rubygems'
require 'bud'
require 'cidr/quorum_delivery'


rd  = ReliableDelivery.new("localhost", 12346)
rd.run
