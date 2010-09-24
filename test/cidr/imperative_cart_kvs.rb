require 'rubygems'
require 'bud'

require 'cidr/kvs.rb'

class ImperativeCartServer < BudKVS

  def state
    super
    channel :iaction, 0, ['server', 'client', 'session', 'item', 'action', 'reqid']
    channel :checkout, 0, ['server', 'client', 'session']

    scratch :client_action, ['server', 'client', 'session', 'item', 'action', 'reqid']
    table :iresponse, ['server', 'client', 'session', 'state']
  end
 
  declare
    def accumulate
      #kvfetch <= iaction.map{|a| [a.server, a.session]}
      # I know my store is local, so I don't bother with fetch...
      joldstate = join [bigtable, iaction], [bigtable.key, iaction.session]
      
      kvstore <+ joldstate.map do |b, a| 
        if a.action == "A"
          [a.server, a.session, b.value.push(a.update)]
        elsif a.action == "D"
          b.value.delete(a.item)
          [a.server, a.session, b.value]
        end

      end

      kvstore <+ iaction.map do |a| 
        unless bigtable.map{|b| b.key}.contains? a.session
          if a.action == "A"
            [a.server, a.session, [a.update]]
          elsif a.action == "D"
            # um, problem with the naive implementation
            print "Ah crap\n"
          end
        end
      end

    end

 
  declare
    def finish
      #iresponse <= join([istatus, checkout], [istatus.session, checkout.session]).map do |s, c|
      #  print "try response\n"
        #[c.client, c.server, s.session, s.state]
      #end
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

