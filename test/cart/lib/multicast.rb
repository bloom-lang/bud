require 'rubygems'
require 'bud'

require 'lib/reliable_delivery'
require 'lib/voting'

module Multicast 
  include Anise
  annotator :declare  

  def state
    super
    table :members, ['peer']
    scratch :send_mcast, ['id'], ['payload']
    table :mcast_done, ['id'], ['payload']
  end

  declare   
  def snd_mcast
    pipe <= join([send_mcast, members]).map do |s, m|
      [m.peer, @addy, s.id, s.payload]
    end
  end
  
  declare 
  def done_mcast
    mcast_done <= pipe_out.map{|p| [p.id, p.payload] }
  end

end

module BestEffortMulticast
  include BestEffortDelivery 
  include Multicast
end

module ReliableMulticast
  include ReliableDelivery
  include VotingMaster
  include VotingAgent
  include Multicast
  include Anise
  annotator :declare

  #declare
  #def glue

  #end
end
