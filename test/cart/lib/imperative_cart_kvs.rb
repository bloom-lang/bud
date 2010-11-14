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
    table :checkout_msg_guard, ['server', 'client', 'session', 'reqid']
  end

  def delete_one(arr, item)
    c = arr.clone
    print "delete #{item} in array #{c.join(",")}\n"
    c.delete_at(c.index(item))
    c
  end
 
  declare
    def queueing
      checkout_msg_guard <= checkout_msg.map{|c| c}

      kvstore <= action_msg.map do |a| 
        if a.action == "A" and !bigtable.map{|b| b.key}.include? a.session
          puts "STORE: " + a.inspect or [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
        end
      end

      joldstate = join [bigtable, action_msg], [bigtable.key, action_msg.session]
      kvstore <= joldstate.map do |b, a| 
        if a.action == "A"
          print "APPEND ("  + @budtime.to_s + ") : " + a.inspect + ", " + b.inspect + "\n" or [a.server, a.client, a.session, a.reqid, (b.value.clone.push(a.item))]
        elsif a.action == "D"
          [a.server, a.client, a.session, a.reqid, delete_one(b.value, a.item)]
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

