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

ip, port = ARGV[0].split(':')
program = ChatServer.new(:ip => ip, :port => port.to_i)
program.run
