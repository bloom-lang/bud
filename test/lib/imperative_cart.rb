require 'rubygems'
require 'bud'
require 'lib/cart'

class ImperativeCartServer < BasicCartServer

  def state
    super
    channel :iaction, 0, ['server', 'client', 'session', 'update']
    channel :checkout, 0, ['server', 'client', 'session']
    channel :rep_istatus, 0, ['server', 'client', 'session'], ['state']

    #blackboard :status, ['server', 'client', 'session'], ['state']
    table :istatus, ['server', 'client', 'session'], ['state']
    table :member, ['player']
    #serializer :queue, ['time', 'server', 'client', 'session', 'update']
  end
 
  declare
    def accumulate
      istatus <+ iaction.map do |q|
        [q.server, q.client, q.session, q.update]
      end

      j = join [iaction, istatus], [iaction.session, istatus.session]  
      #istatus <- join([iaction, status], [iaction.session, status.session]).map{|a, s| s}
      istatus <- j.map{|a, s| s}
    end

 
  declare
    def finish
      response <= join([istatus, checkout], [istatus.session, checkout.session]).map do |s, c|
        [c.client, c.server, s.session, s.item. s.cnt]
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

  declare 
    def replicate
      sj = join [istatus, member]
      rep_istatus <+ sj.map{|s, m| s}
      istatus <= rep_istatus.map{|s| s}
    end
  
end

