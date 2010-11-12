require 'rubygems'
require 'bud'

require 'lib/multicast'

module BasicCartServer
  include Anise
  include Multicast
  annotator :declare

  
  def state
    table :cart_action, ['session', 'item', 'action', 'reqid']
    table :action_cnt, ['session', 'item', 'action'], ['cnt']
    scratch :status, ['server', 'client', 'session', 'item'], ['cnt']
    table :member, ['player']
    table :acked, ['server', 'peer', 'reqid']

    table :memory, ['client', 'server', 'session', 'item', 'cnt']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :client_checkout, ['@server', 'client', 'session', 'reqid']
    scratch :ac, ['session', 'item', 'action', 'reqid']

    channel :ack, ['@server', 'peer', 'reqid']
    channel :action_msg, ['@server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout_msg, ['@server', 'client', 'session', 'reqid']
    channel :response_msg, ['@client', 'server', 'session', 'item', 'cnt']
    channel :tickler, ['@server']
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

  declare 
    def replicate
      send_mcast <= action_msg.map{|a| [a.reqid, [a.session, a.item. a.action]] } 
      action_msg <+ mcast_doen
      
    end

  #declare
  #  def replicate
  #    #action_msg <+ join([action_msg, member]).map do |a, m|
  #   #  unless acked.map{|ac| [ac.peer, ac.reqid]}.include? [m.player, a.reqid]
  ##    #    [m.player, a.server, a.session, a.item, a.action, a.reqid]
  #    #  end
  #    #end
  #  end

  declare
    def client
      action_msg <~ client_action.map{|a| a}
      checkout_msg <~ client_checkout.map{|a| a}
  
      memory <= response_msg.map{|r| r}
    end
end

