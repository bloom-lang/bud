require 'rubygems'
require 'bud'

class ImperativeCartServer < Bud

  def state
    channel :action, ['@server', 'client', 'session', 'update']
    channel :checkout, ['@server', 'client', 'session']

    blackboard :status, ['server', 'client', 'session'], ['state']
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

