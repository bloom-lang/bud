require 'rubygems'
require 'bud'

require 'cidr/async_kvs'
require 'cidr/queue'

class ImperativeCartServer < AsyncKVS

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
    channel :checkout, 0, ['server', 'client', 'session']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :response, 0, ['client', 'server', 'session', 'state']

    scratch :iaction_deq, ['server', 'client', 'session', 'item', 'action', 'reqid']
  end
 
  declare
    def accumulate
      #@q.q <= iaction.map do |a| 
      #  #print "(#{@budtime}) enqueue #{a.inspect}\n"
      #  [a.reqid, a]
      #end
      #iaction_deq <= @q.head.map do |h| 
      #  #print "DEQ(#{@budtime})!: #{h.inspect} (PL #{h.payload})\n"; 
      #  h.payload
      #end

      #kvstore <= iaction_deq.map do |a| 
      kvstore <= iaction.map do |a| 
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
            [a.server, 'localhost:10000', a.session, Array.new.push(a.item)]
          elsif a.action == "D"
            # um, problem with the naive implementation?
            #print "Ah crap\n"
            [a.server, 'localhost:10000', a.session, Array.new]
          end
        end
      end
    end

  declare 
    def artifact
      #kvfetch <= iaction.map{|a| [a.server, a.session]}
      # I know my store is local, so I don't bother with fetch...
      #joldstate = join [bigtable, iaction_deq], [bigtable.key, iaction_deq.session]
      joldstate = join [bigtable, iaction], [bigtable.key, iaction.session]
      
      kvstore <= joldstate.map do |b, a| 

        if a.action == "A"
          #print "add #{a.inspect}, #{b.inspect}\n"
          [a.server, 'localhost:10000', a.session, b.value.push(a.item)]
        elsif a.action == "D"
          #print "delete #{a.inspect}, #{b.inspect}\n"
          #copy = b.value.clone;
          #copy.delete_at(copy.index(a.item));
          #print "now I have #{b.value}\n"
          ### FIX MEE! just to avoid breaking the analysis
          [a.server, 'localhost:10000', a.session, b.value.clone]
        end
      end
      
      ##@q.consumed <=       
    end

 
  declare
    def finish
      response <+ join([bigtable, checkout], [bigtable.key, checkout.session]).map do |s, c|
        #print "try response\n"
        [c.client, c.server, s.key, s.value]
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

