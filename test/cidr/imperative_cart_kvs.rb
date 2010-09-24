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
      kvstore <= iaction.map do |a| 
        print "Around #{a.inspect}\n"
        unless bigtable.map{|b| b.key}.include? a.session
          if a.action == "A"
            print "add on empty #{a.server}, #{a.session}, #{a.item}\n"
            [a.server, a.session, [a.item]]
          elsif a.action == "D"
            # um, problem with the naive implementation?
            print "Ah crap\n"
          end
        end
      end
    end

  declare 
    def artifact
      #kvfetch <= iaction.map{|a| [a.server, a.session]}
      # I know my store is local, so I don't bother with fetch...
      joldstate = join [bigtable, iaction], [bigtable.key, iaction.session]
      
      kvstore <= joldstate.map do |b, a| 
        if a.action == "A"
          print "add #{a.inspect}, #{b.inspect}\n"
          [a.server, a.session, b.value.push(a.item)]
        elsif a.action == "D"
          print "delete #{a.inspect}, #{b.inspect}\n"
          b.value.delete_at(b.value.index(a.item))
          print "now I have #{b.value}\n"
          [a.server, a.session, b.value]
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

