require 'rubygems'
require 'bud'

# make the checkout message idempotent

require 'examples/cart'

class IdempotentCartServer < BasicCartServer

  def state
    super
    table :my_status, ['session', 'item'], ['cnt'], 'first'

    scratch :protected_checkout, ['server', 'client', 'session']
    table :checkout_log, ['server', 'client', 'session'], ['time'], 'first' 
    scratch :checkout_event, ['server', 'client', 'session', 'time']
  end
  
  def declaration
    super
    strata[2] = rules {
      checkout_event <= protected_checkout.map do |c| 
        unless checkout_log.map{|l| l.session}.include? c.session
          print "checkout EVENT\n"
          [c.server, c.client, c.session, Time.new.to_i]
        end
      end
     checkout_log <= checkout_event.map{|e| e}

     checkout <+ checkout_event.map{|e| [e.server, e.client, e.session]}

      my_status <= status.map{|s| s}
    }
  end
end


