require 'rubygems'
require 'bud'

require 'lib/kvs_metered'
require 'lib/queue'
require 'lib/cart_protocol'

module DestructiveCart
  include Anise
  include CartProtocol
  include KVSProtocol
  annotator :declare

  def state
    super
    scratch :can_act, ['server', 'client', 'session', 'reqid', 'item', 'action']
  end

  def delete_one(arr, item)
    c = arr.clone
    print "delete #{item} in array #{c.join(",")}\n"
    c.delete_at(c.index(item))
    c
  end

  declare 
  def indirection
    can_act <= action_msg.map{|a| a } 
    kvget <= can_act.map {|a| [a.reqid, a.session] } 
  end
 
  declare
    def queueing
      kvput <+ can_act.map do |a| 
        if a.action == "A" and !kvget_response.map{|b| b.key}.include? a.session
          puts "STORE: " + a.inspect or [a.client, a.session, a.reqid, Array.new.push(a.item)]
        end
      end

      joldstate = join [kvget_response, can_act], [kvget_response.key, can_act.session]
      kvput <+ joldstate.map do |b, a| 
        if a.action == "A"
          puts " APPEND ("  + @budtime.to_s + ") : " + a.inspect + ", " + b.inspect + "\n" or [a.server, a.client, a.session, a.reqid, (b.value.clone.push(a.item))]
        elsif a.action == "D"
          puts @budtime.to_s + "DELETE " + a.inspect + " from " + b.inspect or [a.server, a.client, a.session, a.reqid, delete_one(b.value, a.item)]
        end
      end
    end

  declare
    def finish
      response_msg <~ join([kvget_response, checkout_msg], [kvget_response.key, checkout_msg.session]).map do |s, c|
        [c.client, c.server, s.key, s.value]
      end
    end
end


module MeteredDestructiveCart
  include DestructiveCart
  include BaseQueue
  include Anise
  annotator :declare

  declare
  def indirection
    q <= action_msg.map{|a| [a.reqid, a] }
    can_act <= head.map{|h| h.payload }  
    consumed <= head.map{|h| [h.ident] }    
  end  
end
