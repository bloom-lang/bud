require 'chat_client'

# ip, port = ARGV[0].split(':')
program = ChatClient.new(ARGV[0], ARGV[1], {:read_stdin => true})
program.run
