require 'chatserver'

ip, port = ARGV[0].split(':')
program = ChatServer.new({:ip => ip, :port => port.to_i})
program.run
