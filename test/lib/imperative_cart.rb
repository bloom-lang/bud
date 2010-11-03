require 'rubygems'
require 'bud'
require 'lib/cart'

##class ImperativeCartServer < BasicCartServer
class ImperativeCartServer < Bud

  def state
    #super
    channel :iaction, 0, ['server', 'client', 'session', 'update']
    channel :checkout, 0, ['server', 'client', 'session']
    channel :rep_istatus, 0, ['server', 'client', 'session'], ['state']
    channel :tickler, 0, ['server']

    #blackboard :status, ['server', 'client', 'session'], ['state']

    # duplicated:
    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']

    table :iresponse, ['server', 'client', 'session', 'state']
    table :istatus, ['server', 'client', 'session'], ['state']
    table :member, ['player']
    #serializer :queue, ['time', 'server', 'client', 'session', 'update']
  end
 
  declare
    def accumulate
      istatus <+ iaction.map do |q|
        print "GOT IA\n"
        [q.server, q.client, q.session, q.update]
      end

      j = join [iaction, istatus], [iaction.session, istatus.session]  
      istatus <- j.map{|a, s| print "ready to delete status\n"; s}
    end

 
  declare
    def finish
      iresponse <= join([istatus, checkout], [istatus.session, checkout.session]).map do |s, c|
        print "try response\n"
        [c.client, c.server, s.session, s.state]
      end
    end

  declare 
    def client
      iaction <~ client_action.map{|a| a}
    end

  declare
    def consider
      # empty; clobber parent
        
    end
  declare
    def acks
      
    end

  declare 
    def replicate
      #sj = join [iaction, member]
      #rep_istatus <~ sj.map{|s, m| [m.player, s.server, s.session, s.update] }
      #istatus <= rep_istatus.map{|s| s}
    end
  
end

