require 'rubygems'
require 'bud'

module ChatProtocol
  def chat_protocol_state
    channel :mcast, ['@to', 'from', 'username', 'time'], ['msg']
    channel :ctrl, ['@to', 'from', 'cmd']
    terminal :term
  end
end
