require 'chat_master'

ip, port = ARGV[0].split(':')
program = ChatMaster.new({:ip => ip, :port => port.to_i})
program.run
