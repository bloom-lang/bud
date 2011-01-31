# simple chat
# run "ruby run_chat_master.rb 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby run_chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'chat'

source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = ChatClient.new(ARGV[1], ARGV[2],
                         :ip => ip, :port => port, :visualize => true, :read_stdin => true)
program.run
