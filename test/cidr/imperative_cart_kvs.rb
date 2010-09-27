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
    channel :iaction, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout, 0, ['server', 'client', 'session', 'reqid']
    channel :response, 0, ['client', 'server', 'session', 'state']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    scratch :iaction_deq, ['server', 'client', 'session', 'item', 'action', 'reqid']

    # to know when to check out
    #table :action_log, ['server', 'client', 'session', 'item', 'action', 'reqid']
    #table :max_act, ['server', 'client', 'session', 'maxreq']
  end
 
  declare
    def queueing
      @q.q <= iaction.map do |a| 
        [a.reqid, a]
      end
      iaction_deq <= @q.head.map do |h| 
        print "DEQ(#{@budtime})!: (PL #{h.payload})\n"; 
        h.payload
      end

      @q.consumed <= iaction_deq.map do |a|
        unless bigtable.map{|b| b.key}.include? a.session
          print "Consume1\n"
          [a.reqid]
        end
      end

      #action_log <= iaction.map{|a| a}
      #max_act <= action_log.group([action_log.server, action_log.client, action_log.session], max(action_log.reqid))
      #checkout_guard <= checkout.map{|c| c}
    end
  declare 
    def accumulate
      kvstore <= iaction_deq.map do |a| 
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
	          print "ADD ON #{a.session}, #{a.item}\n"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new.push(a.item)]
          elsif a.action == "D"
            # um, problem with the naive implementation?
            #print "Ah crap\n"
		        print "ADD on empty #{a.session}, [] \n"
            [a.server, 'localhost:10000', a.session, a.reqid, Array.new]
          end
        end
      end

      joldstate = join [bigtable, iaction_deq], [bigtable.key, iaction_deq.session]
      #joldstate = join [bigtable, iaction], [bigtable.key, iaction.session]
     
      #print  "IAD len #{iaction_deq.length}, btlen
      kvstore <= joldstate.map do |b, a| 
        print "UGGU\n"
        if a.action == "A"
          print "add #{a.inspect}, #{b.inspect}\n"
          [a.server, a.client, a.session, a.reqid, b.value.push(a.item)]
        elsif a.action == "D"
      #    print "delete #{a.inspect}, #{b.inspect}\n"
          copy = b.value.clone;
          copy.delete_at(copy.index(a.item));
      #    #print "now I have #{b.value}\n"
      #    ### FIX MEE! just to avoid breaking the analysis
      #    [a.server, 'localhost:10000', a.session, a.reqid, b.value.clone]
          [a.server, a.client, a.session, a.reqid, copy]
        end
      end

      # I'd rather tuck this in the 'queueing' block, but don't want to reevaluate the join
      @q.consumed <= joldstate.map do |b, a|
        print "CONSUME #{a.inspect}\n"
        [a.reqid] 
      end
    end

 
  declare
    def finish
      ##response <+ join([bigtable, checkout_guard, max_act], [bigtable.key, checkout_guard.session], [checkout_guard.session, max_act.session]).map do |s, c, m|
      response <+ join([bigtable, checkout], [bigtable.key, checkout.session]).map do |s, c|
        print "RESPONSE #{s.inspect}, #{c.inspect}\n"
        #[c.client, c.server, s.key, s.value]
      end
    end

  declare 
    def client
      iaction <+ client_action.map{|a| a}
    end

  declare
    def consider
      # empty; clobber parent
        
    end
  declare
    def acks
      
    end

end

