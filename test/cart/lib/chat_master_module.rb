# simple chat
# run "ruby lib/chat_master_module.rb 127.0.0.1:12345"
# run "ruby lib/chat_module.rb 127.0.0.1:12346 alice 127.0.0.1:12345"
# run "ruby lib/chat_module.rb 127.0.0.1:12347 bob 127.0.0.1:12345"
# run "ruby lib/chat_module.rb 127.0.0.1:12348 harvey 127.0.0.1:12345"
require 'rubygems'
require 'bud'
require 'lib/chat_protocol'
require 'lib/2pc'
require 'lib/chat_master_base'


# safe chat master extends chatmaster and mixes in 2pcmaster
class SafeChatMaster < ChatMaster
  include TwoPCMaster

  def state
    super if defined? super
    scratch :empty_echo, ['xid']
  end

  declare
  def shuffle
    # add chatters to our 2pc member list
    member <= ctrl.map {|c| [c.from] }
  end
  
  declare
  def shutdown
    empty_echo <= xact.map do |x|
      if x.status == "commit"
        raise "Cleanly exit? #{x.data}"
      end
    end
  end
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = SafeChatMaster.new(ip, port, {'visualize' => false, 'dump' => true})

program.run_bg
sleep 10
program.request_commit <+ [[ 123, 'shutdown' ]] 

sleep 60
