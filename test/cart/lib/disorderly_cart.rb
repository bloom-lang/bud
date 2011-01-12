require 'rubygems'
require 'bud'

require 'lib/multicast'
require 'lib/cart_protocol'

module DisorderlyCart
  include Anise
  include CartProtocol
  annotator :declare

  def state
    super
    table :cart_action, ['session', 'reqid'], ['item', 'action']
    scratch :action_cnt, ['session', 'item', 'action'], ['cnt']
    scratch :status, ['server', 'client', 'session', 'item'], ['cnt']
  end
 
  declare
  def saved
    # store actions against the "cart;" that is, the session.
    cart_action <= action_msg.map { |c| [c.session, c.reqid, c.item, c.action] }
    action_cnt <= cart_action.group([cart_action.session, cart_action.item, cart_action.action], count(cart_action.reqid))
  end

  declare
  def consider
    status <= join([action_cnt, action_cnt, checkout_msg]).map do |a1, a2, c| 
      if a1.session == a2.session and a1.item == a2.item and a1.session == c.session and a1.action == "Add" and a2.action == "Del" 
        if (a1.cnt - a2.cnt) > 0
          puts "STAT" or [c.client, c.server, a1.session, a1.item, a1.cnt - a2.cnt] 
        end
      end
    end
    status <= join([action_cnt, checkout_msg]).map do |a, c|
      if a.action == "Add" and not action_cnt.map{|d| d.item if d.action == "Del"}.include? a.item
        [c.client, c.server, a.session, a.item, a.cnt]
      end
    end

    #response_msg <~ status.group([status.client, status.server, status.session], accum((0..(status.cnt)).map{status.item})) 
    #response_msg <~ status.group([status.client, status.server, status.session], accum([status.item, status.cnt]))
    response_msg <~ status.group([status.client, status.server, status.session], accum(status.item))
  end
end

module ReplicatedDisorderlyCart
  include DisorderlyCart
  include Multicast
  include Anise
  annotator :declare

  declare 
  def replicate
    send_mcast <= action_msg.map {|a| [a.reqid, [a.session, a.reqid, a.item, a.action]] }
    cart_action <= mcast_done.map {|m| m.payload } 
    cart_action <= pipe_chan.map{|c| c.payload }
  end    
end


