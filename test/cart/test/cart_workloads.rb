require 'rubygems'
require 'bud'
require 'test/test_lib'

module CartWorkloads
  def run_cart(program)
    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'A', 123])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'A', 124])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'diapers', 'A', 125])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'D', 126])

    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'A', 127])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'A', 128])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'A', 129])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'D', 130])


    send_channel(program.ip, program.port, "checkout_msg", [addy, addy,1234, 131])
    advance(program)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'papers', 'A', 132])
    advance(program)    
  end
end
