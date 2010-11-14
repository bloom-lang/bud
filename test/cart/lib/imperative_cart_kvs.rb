require 'rubygems'
require 'bud'

require 'lib/kvs_metered'
require 'lib/queue'
require 'lib/cart_protocol'

module DestructiveCart
  include Anise
  include CartProtocol
  include BudKVS
  annotator :declare

  def state
    super
    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    scratch :action_msg_deq, ['server', 'client', 'session', 'item', 'action', 'reqid']
    table :checkout_msg_guard, ['server', 'client', 'session', 'reqid']
    # to know when to check out
    table :action_log, ['server', 'client', 'session', 'item', 'action', 'reqid']
  end
 
  declare
    def queueing
      checkout_msg_guard <= checkout_msg.map{|c| c}

      kvstore <= action_msg.map do |a| 
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
            #puts "ADD ON " + a.session.to_s + ", " + a.item.to_s or [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
          #elsif a.action == "D"
            # um, problem with the naive implementation?
            #[a.server, 'localhost:10000', a.session, a.reqid, Array.new]
          end
        end
      end

      joldstate = join [bigtable, action_msg], [bigtable.key, action_msg.session]
      kvstore <= joldstate.map do |b, a| 
        if a.action == "A"
          print "APPEND ("  + @budtime.to_s + ") : " + a.inspect + ", " + b.inspect + "\n" or [a.server, a.client, a.session, a.reqid, (b.value.clone.push(a.item))]

        elsif a.action == "D"
      #    #print "delete #{a.inspect}, #{b.inspect}\n"
          [a.server, a.client, a.session, a.reqid, b.value.reject{|i| i == a.item}]
        end
      end
    end

  #declare
  #  def finish
  #    ##response <~ join([bigtable, checkout_msg_guard, max_act], [bigtable.key, checkout_msg_guard.session], [checkout_msg_guard.session, max_act.session]).map do |s, c, m|
  #    response <~ join([bigtable, checkout_msg_guard], [bigtable.key, checkout_msg_guard.session]).map do |s, c|
  #      #print "RESPONSE #{s.inspect}, #{c.inspect}\n"
  #      [c.client, c.server, s.key, s.value]
  #    end
  #  end

end

