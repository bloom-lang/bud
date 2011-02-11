require 'rubygems'
require 'bud'
require 'chat_protocol'
require '2pc'
require 'chat_master_base'

# safe chat master extends chatmaster and mixes in 2pcmaster
class SafeChatMaster < ChatMaster
  include TwoPCMaster

  def state
    super
    scratch :halt, [:xid]
  end

  declare
  def shuffle
    # add chatters to our 2pc member list
    member <= ctrl.map {|c| [c.from] }
  end
  
  declare
  def shutdown
    halt <+ xact.map do |x|
      [x.xid] if x.status == "commit"
    end
    halt <+ halt.map {|h| exit}
  end
end
