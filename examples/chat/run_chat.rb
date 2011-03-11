require 'chat_client'

ip, port = ARGV[0].split(':')
program = ChatClient.new(ARGV[1], ARGV[2],
                        {:ip => ip, :port => port.to_i, :read_stdin => true})
program.run
