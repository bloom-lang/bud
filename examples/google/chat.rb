require 'rubygems'
require 'bud'
require 'chat_protocol'
require '2pc'
require 'chat_base'

class SafeChatClient < ChatClient
  include TwoPCAgent

  declare
  def shutd
    stdio <~ ballot.map{|b| ["Shutdown request.  type 'OK' to accept"] }
    can_commit <= join([stdio, waiting_ballots]).map{ |t, w| [w.id, "Y"] if t == ["OK"] }
  end  
end
