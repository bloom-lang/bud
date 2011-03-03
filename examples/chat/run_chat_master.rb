# simple chat
# run "ruby run_chat_master.rb 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'chat_master'

ip, port = ARGV[0].split(':')
program = ChatMaster.new({:ip => ip, :port => port.to_i})
program.run
