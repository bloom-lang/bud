require 'rubygems'
require 'bud'

require 'cidr/analysis_digests/pedagogical_kvs'
require 'cidr/analysis_digests/queue'

class ImperativeCartServer < BudKVS

  def initialize(ip, port)
    #@q = BaseQueue.new(ip, port.to_i+1)
    #@q.tick
    super(ip, port)
  end

  def tick
    # hx
    #@q.tick
    super
  end

  def state
    super
    channel :action_msg, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout_msg, 0, ['server', 'client', 'session', 'reqid']
    channel :response_msg, 0, ['client', 'server', 'session', 'state']
    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
  end
 
  declare 
    def queueing
      kvput <= action_msg.map do |a| 
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
          elsif a.action == "D"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new]
          end
        end
      end

      kvput <= join([bigtable, action_msg]).map do |b, a| 
        if b.key == a.session
          if a.action == "A"
            [a.server, a.client, a.session, a.reqid, b.value.push(a.item)]
          elsif a.action == "D"
            #copy = b.value.clone;
            #copy.delete_at(copy.index(a.item));
            [a.server, 'localhost:10000', a.session, a.reqid, b.value.clone]
      #    [a.server, a.client, a.session, a.reqid, copy]
          end
        end
      end
    end

 
  declare 
    def finish
      response_msg <+ join([bigtable, checkout_msg]).map do |s, c|
        if s.key = c.session
          [c.client, c.server, s.key, s.value]
        end
      end
    end

  declare 
    def client
      action_msg <+ client_action.map{|a| a}
    end


end

