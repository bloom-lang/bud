require 'rubygems'
require 'bud'

class BasicCartServer < Bud

  def state
    table :cart_action, ['session', 'item', 'action', 'reqid']
    table :action_cnt, ['session', 'item', 'action'] , ['cnt']
    table :status, ['session', 'item'], ['cnt']
	
    scratch :ac, ['session', 'item', 'action', 'reqid']

    channel :action, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout, 0, ['server', 'client', 'session']
  end
  
  def declaration
    strata[0] = rules {
      # store actions against the "cart;" that is, the session.
      cart_action <= action.map { |c| [c.session, c.item, c.action, c.reqid] }
      # do I have to split the join-agg into 2 strata?
      j = join [ cart_action, checkout ], [cart_action.session, checkout.session]
      ac <= j.map do | a, c | 
        [a.session, a.item, a.action, a.reqid] #if a.session = c.session
      end
    }

    strata[1] = rules {
      action_cnt <= ac.group([ac.session, ac.item, ac.action], count(ac.reqid))
      action_cnt <= ac.map{|a| [a.session, a.item, 'D', 0] unless ac.map{|c| [c.session, c.item] if c.action == "D"}.include? [a.session, a.item]}

      status <= join([action_cnt, action_cnt, checkout]).map do |a1, a2, c| 
        if a1.session == a2.session and a1.item == a2.item and a1.session == c.session and a1.action == "A" and a2.action == "D"
          [a1.session, a1.item, a1.cnt - a2.cnt] if (a1.cnt - a2.cnt) > 0
        end
      end
    }
  end
end

class CartClient < Bud
  def state
  
  end

  def declaration

  end
end

