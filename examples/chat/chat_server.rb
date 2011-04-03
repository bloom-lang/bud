require 'rubygems'
require 'bud'
require 'chat_protocol'

class ChatServer
  include Bud
  include ChatProtocol

  state { table :nodelist }

  bloom do
    nodelist <= connect.payloads
    mcast <~ (mcast * nodelist).pairs { |m,n| [n.key, m.val] }
  end
end



# ruby command-line wrangling
if ARGV.first
  addr = ARGV.first
else
  addr = ChatProtocol::DEFAULT_ADDR
end

ip, port = addr.split(":")
puts "Server address: #{ip}:#{port}"
program = ChatServer.new(:ip => ip, :port => port.to_i)
program.run_fg
