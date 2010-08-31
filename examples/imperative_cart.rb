require 'rubygems'
require 'bud'

class ImperativeCartServer < Bud

  def state
    channel :action, 0, ['server', 'client', 'session', 'update']
    channel :checkout, 0, ['server', 'client', 'session']

    table :status, ['server', 'client', 'session'], ['state'], 'last'
    serializer :queue, ['time', 'server', 'client', 'session', 'update']
  end
  
  def declaration
    strata[0] = rules {
      queue <= action.map{|a| [@budtime, a.server, a.client, a.session, a.update]}
      status <= queue.map do |q|
        [q.server, q.client, q.session, q.update]
      end

    }
  end
end

