require 'rubygems'
require 'bud'

require 'lib/reliable_delivery'
require 'lib/voting'

module Multicast 
  include Anise
  annotator :declare  
  include DeliveryProtocol

  def state
    super
    table :members, ['peer']
    scratch :send_mcast, ['ident'], ['payload']
    scratch :mcast_done, ['ident'], ['payload']
  end

  declare   
  def snd_mcast
    pipe_in <= join([send_mcast, members]).map do |s, m|
      [m.peer, @addy, s.ident, s.payload]
    end
  end
  
  declare 
  def done_mcast    
    # override me
    mcast_done <= pipe_out.map{|p| [p.ident, p.payload] }
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

  declare
  def start_mcast
    begin_vote <= send_mcast.map{|s| [s.ident, s] }
  end

  declare
  def agency
    cast_vote <= join([pipe_out, waiting_ballots], [pipe_out.ident, waiting_ballots.ident]).map{|p, b| [b.ident, b.content]} 
  end

  declare
  def done_mcast
    mcast_done <= vote_status.map do |v|
      puts "VEE: " + v.inspect 
    end
  end
end
