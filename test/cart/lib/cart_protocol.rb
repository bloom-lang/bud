require 'rubygems'
require 'bud'

module CartProtocol
  def state
    super
    # PAA -- took the '@'s off all occurrences of 'server' below
    channel :action_msg, 
      ['server', 'client', 'session', 'reqid'], ['item', 'action']
    channel :checkout_msg, 
      ['server', 'client', 'session', 'reqid']
    channel :response_msg, 
      ['client', 'server', 'session', 'item'], ['cnt']
  end
end

module CartClientProtocol
  def state
    super
    interface input, :client_checkout, ['server', 'session', 'reqid']
    interface input, :client_action, ['server', 'session', 'reqid'], ['item', 'action']
    interface output, :client_response, ['client', 'server', 'session'], ['item', 'cnt']
  end
end

module CartClient
  include CartProtocol
  include CartClientProtocol
  include Anise
  annotator :declare

  def state
    super
    internal output, :action_msg
    internal input, :client_checkout
    internal input, :response_msg
  end

  declare 
  def client
    action_msg <~ client_action.map{|a| puts "server is "+a.server.to_s or [a.server, @addy, a.session, a.reqid, a.item, a.action]}
    checkout_msg <~ client_checkout.map{|a| [a.server, @addy, a.session, a.reqid]}
    client_response <= response_msg.map {|r| r }
  end
end
