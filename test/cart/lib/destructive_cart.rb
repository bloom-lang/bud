require 'rubygems'
require 'bud'

require 'lib/kvs'
require 'lib/cart_protocol'


module DestructiveCart
  include CartProtocol
  include KVSProtocol
  include Anise 
  annotator :declare

  declare
  def queueing
    kvget <= action_msg.map {|a| puts "test" or [a.reqid, a.session] }
    kvput <= action_msg.map do |a| 
      if a.action == "Add" and not kvget_response.map{|b| b.key}.include? a.session
        puts "PUT EMPTY" or [a.client, a.session, a.reqid, Array.new.push(a.item)]
      end
    end

    old_state = join [kvget_response, action_msg], [kvget_response.key, action_msg.session]
    kvput <= old_state.map do |b, a| 
      if a.action == "Add"
        [a.client, a.session, a.reqid, (b.value.clone.push(a.item))]
      elsif a.action == "Del"
        [a.client, a.session, a.reqid, delete_one(b.value, a.item)]
      end
    end
  end

  declare
  def finish
    kvget <= checkout_msg.map{|c| [c.reqid, c.session] }
    lookup = join([kvget_response, checkout_msg], [kvget_response.key, checkout_msg.session])
    response_msg <~ lookup.map do |r, c|
      puts "RESP" or [r.client, r.server, r.key, r.value, nil]
    end
  end
end
