# simple chat
require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatServer
  include Bud
  include ChatProtocol

  state do
    table :nodelist
  end

  bloom :server_logic do
    nodelist <= connect.payloads
    mcast <~ join([mcast, nodelist]) do |m,n| 
      [n.key, m.val] unless n.key == m.val[0]
    end
  end
end

if ARGV.first
  addr = ARGV.first
else
  addr = ChatProtocol::DEFAULT_ADDR
end

ip, port = addr.split(":")
puts "Server address: #{ip}:#{port}"
program = ChatServer.new(:ip => ip, :port => port.to_i)
program.run
