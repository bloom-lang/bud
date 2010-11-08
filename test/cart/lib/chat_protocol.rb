require 'rubygems'
require 'bud'

module ChatProtocol
  def chat_protocol_state
    channel :mcast, ['@to', 'from', 'nick', 'time'], ['msg']
    channel :ctrl, ['@to', 'from', 'cmd']
    stdio :term
  end

  #declare   
  #def foo
  #  ctrl <= term.map{|t| t}
  #end
end
