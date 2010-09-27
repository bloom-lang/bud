require 'rubygems'
require 'bud'

class BasicCartServer < Bud

  def state
    table :cart_action, ['session', 'item', 'action', 'reqid']
    table :action_cnt, ['session', 'item', 'action'] , ['cnt']
    table :status, ['session', 'item'], ['cnt']
    table :member, ['player']
    #table :acked, ['server', 'peer', 'reqid']

    # this was the guard
    #table :checkout, ['server', 'client', 'session']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    scratch :ac, ['session', 'item', 'action', 'reqid']

    #channel :ack, 0, ['server', 'peer', 'reqid']
    channel :action, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout, 0, ['server', 'client', 'session']
    channel :response, 0, ['client', 'server', 'session', 'item', 'cnt']
    channel :tickler, 0, ['server']
  end
 
  declare
    def accumulate 
      # store actions against the "cart;" that is, the session.
      cart_action <= action.map { |c| [c.session, c.item, c.action, c.reqid] }

      # do I have to split the join-agg into 2 strata?
      j = join [ cart_action, checkout ], [cart_action.session, checkout.session]
      ac <= j.map do | a, c | 
	print "load AC #{a.inspect}\n"
        [a.session, a.item, a.action, a.reqid] #if a.session = c.session
      end

      #checkout <= checkout.map{|c| c}

      action_cnt <= ac.group([ac.session, ac.item, ac.action], count(ac.reqid))
      action_cnt <= ac.map{|a| [a.session, a.item, 'D', 0] unless ac.map{|c| [c.session, c.item] if c.action == "D"}.include? [a.session, a.item]}

    end

  declare 
    def acks
      #ack <+ action.map {|a| [a.client, a.server, a.reqid] }
      #acked <= ack.map{|a| a}
    end

  declare
    def consider
      print "ACLen is #{action_cnt.length}, COlen #{checkout.length}\n"

      # rewrite the following with equijoin preds?? -- JMH  
      status <= join([action_cnt, action_cnt, checkout]).map do |a1, a2, c| 
	  print "p'raps  STATUS recs: #{a1.inspect}, @{a2.inspect}\n"
        if a1.session == a2.session and a1.item == a2.item and a1.session == c.session and a1.action == "A" and a2.action == "D"
	  print "STATUS recs: #{a1.inspect}, @{a2.inspect}\n"
          [a1.session, a1.item, a1.cnt - a2.cnt] if (a1.cnt - a2.cnt) > 0
        end
      end
    end
  declare 
    def finish
      # what does the response channel actually contain? -- JMH
      #response <= join([status, checkout], [status.session, checkout.session]).map do |s, c| 
      #  print "RESPONSE: #{s.inspect}\n"
      #  #[c.client, c.server, s.session, s.item, s.cnt]
      #end
    end
  declare
    def replicate
      #action <+ join([action, member]).map do |a, m|
      #  #unless acked.map{|ac| [ac.peer, ac.reqid]}.include? [m.player, a.reqid]
      #    [m.player, a.server, a.session, a.item, a.action, a.reqid]
      #  #end
      #end

      #checkout <+ join([checkout, member]).map do |c, m|
      #  [m.player, c.client, c.session]
      #end
    end

  declare
    #def client
    #  action <+ client_action.map{|a| a}
    #end
  # um
end

