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
    table :cart_action, ['session', 'item', 'action', 'reqid']
    table :action_cnt, ['session', 'item', 'action'], ['cnt']
    scratch :status, ['server', 'client', 'session', 'item'], ['cnt']
  end
 
  declare
  def saved
    # store actions against the "cart;" that is, the session.
    cart_action <= action_msg.map { |c| [c.session, c.item, c.action, c.reqid] }

    # PAA - CRASH without the +?  find out why.
    action_cnt <+ cart_action.group([cart_action.session, cart_action.item, cart_action.action], count(cart_action.reqid))
    action_cnt <+ cart_action.map{|a| [a.session, a.item, 'D', 0] unless cart_action.map{|c| [c.session, c.item] if c.action == "D"}.include? [a.session, a.item]}

  end

  declare
  def consider
    status <= join([action_cnt, action_cnt, checkout_msg]).map do |a1, a2, c| 
      if a1.session == a2.session and a1.item == a2.item and a1.session == c.session and a1.action == "A" and a2.action == "D" 
        if (a1.cnt - a2.cnt) > 0
          [c.client, c.server, a1.session, a1.item, a1.cnt - a2.cnt] 
        end
      end
    end
    response_msg <~ status.map { |s| s }
  end
end

module ReplicatedDisorderlyCart
  include DisorderlyCart
  include Multicast
  include Anise
  annotator :declare

  declare 
  def replicate
    send_mcast <= action_msg.map {|a| [a.reqid, a] }
    action_msg <= mcast_done.map {|m| m.payload } 
  end    
end


