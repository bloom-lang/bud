require 'rubygems'
require 'bud'

module PingProtocol
  def ping_protocol_state
    channel :flow, ['@otherloc', 'me', 'msg', 'wall', 'budtick']
    terminal :out, ['text']
  end
end
