# simple chat
# run "ruby run_chat_master.rb 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'chat_master'

source = ARGV[0].split(':')
ip = source[0]
port = source[1]
program = ChatMaster.new(:ip => ip, :port => port,
                         :visualize => false, :dump => false)

t = program.run_bg
sleep 10
program.stop_bg
# program.request_commit <+ [[ 123, 'shutdown' ]] 
# sleep 60
