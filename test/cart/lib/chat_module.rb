# simple chat
# run "ruby chat_master.rb 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby chat.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'lib/chat_protocol'
require 'lib/2pc'
require 'lib/chat_base'

class SafeChatClient < ChatClient
  include TwoPCAgent

  def state
    super
    state_2pca
  end
  
  declare
  def shutd
    term <= ballot.map{|b| ["Shutdown request.  type 'OK' to accept"] }
    can_commit <= join([term, waiting_ballots]).map{ |t, w| [w.id, "Y"] if t == ["OK"] }
  end
  
end


source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = SafeChatClient.new(ip, port, ARGV[1], ARGV[2])
program.run
