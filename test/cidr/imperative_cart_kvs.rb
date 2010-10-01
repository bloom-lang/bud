require 'rubygems'
require 'bud'

require 'cidr/kvs'
require 'cidr/queue'

class ImperativeCartServer < BudKVS

  def initialize(ip, port)
    @q = BaseQueue.new(ip, port.to_i+1)
    @q.tick
    super(ip, port)
  end

  def tick
    # hx
    @q.tick
    super
  end

  def state
    super
    channel :action_msg, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout_msg, 0, ['server', 'client', 'session', 'reqid']
    channel :response, 0, ['client', 'server', 'session', 'state']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    scratch :action_msg_deq, ['server', 'client', 'session', 'item', 'action', 'reqid']

    table :checkout_msg_guard, ['server', 'client', 'session', 'reqid']
    # to know when to check out
    table :action_log, ['server', 'client', 'session', 'item', 'action', 'reqid']
    table :memory, ['client', 'server', 'session', 'item', 'cnt']
    #table :max_act, ['server', 'client', 'session', 'maxreq']
  end
 
  declare
    def queueing
      @q.q <= action_msg.map do |a| 
        [a.reqid, a]
      end
      action_msg_deq <= @q.head.map do |h| 
        print "DEQ(#{@budtime})!: (PL #{h.payload})\n"; 
        h.payload
      end



     # @q.consumed <= action_msg_deq.map do |a|
     #   unless bigtable.map{|b| b.key}.include? a.session
     #     print "Consume1\n"
     #     [a.reqid]
     #   end
     # end
      # arguably a violation of encapsulation
      @q.consumed <= pipe_out.map{|p| print "consum\n"; [p.id]}

      action_log <= action_msg.map{|a| a}
      checkout_msg_guard <= checkout_msg.map{|c| c}

      kvstore <= action_msg_deq.map do |a| 
        #print "IAD!\n"
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
	          print "ADD ON #{a.session}, #{a.item}\n"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
          elsif a.action == "D"
            # um, problem with the naive implementation?
            print "Ah crap\n"
		        #print "ADD on empty #{a.session}, [] \n"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new]
          end
        end
      end

      print "cardinality of DQ: #{action_msg_deq.length}\n"

      joldstate = join [bigtable, action_msg_deq], [bigtable.key, action_msg_deq.session]
      #joldstate = join [bigtable, action_msg], [bigtable.key, action_msg.session]
     
      #print  "IAD len #{action_msg_deq.length}, btlen
      kvstore <= joldstate.map do |b, a| 
        print "UGGU #{b.inspect}  < ---  #{a.inspect}\n"
        if a.action == "A"
          print "add(#{@port}:#{@budtime})  #{a.inspect}, #{b.inspect}\n"
          [a.server, a.client, a.session, a.reqid, b.value.push(a.item)]
        elsif a.action == "D"
          print "delete #{a.inspect}, #{b.inspect}\n"
      #    copy = b.value.clone;
      #    copy.delete_at(copy.index(a.item));
      #    #print "now I have #{b.value}\n"
      #    ### FIX MEE! just to avoid breaking the analysis
      #    [a.server, 'localhost:10000', a.session, a.reqid, b.value.clone]
      #    [a.server, a.client, a.session, a.reqid, copy]
        end
      end
    end

  declare
    def done
      print "NEW DONE\n"
      super
    end

 
  declare
    def finish
      ##response <+ join([bigtable, checkout_msg_guard, max_act], [bigtable.key, checkout_msg_guard.session], [checkout_msg_guard.session, max_act.session]).map do |s, c, m|
      #response <+ join([bigtable, checkout_msg_guard], [bigtable.key, checkout_msg_guard.session]).map do |s, c|
      #  #print "RESPONSE #{s.inspect}, #{c.inspect}\n"
      #  [c.client, c.server, s.key, s.value]
      #end
    end

  declare 
    def client
      action_msg <+ client_action.map{|a| a}
    end
end

