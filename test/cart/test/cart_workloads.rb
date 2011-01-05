require 'rubygems'
require 'bud'
require 'test/test_lib'

module CartWorkloads
  def run_cart(program)
    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    print "ADDY iS #{addy}\n"
    program.client_action <+ [[addy, 1234, 123, 'meat', 'Add']]

    program.client_action <+ [[addy, 1234, 124, 'beer', 'Add']]
    program.client_action <+ [[addy, 1234, 125, 'diapers', 'Add']]
    program.client_action <+ [[addy, 1234, 126, 'meat', 'Del']]


    program.client_action <+ [[addy, 1234, 127, 'beer', 'Add']]
    program.client_action <+ [[addy, 1234, 128, 'beer', 'Add']]
    program.client_action <+ [[addy, 1234, 129, 'beer', 'Add']]
    program.client_action <+ [[addy, 1234, 130, 'beer', 'Del']]

    advance(program)
    advance(program)
    advance(program)
    advance(program)
    advance(program)

    program.client_checkout <+ [[addy, 1234, 131]]
    advance(program)

    program.client_action <+ [[addy, 1234, 132, 'papers', 'Add']]
    advance(program)
    
  end

  def run_cart2(program)
    addy = "#{program.ip}:#{program.port}"
    add_members(program, addy)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'Add', 123])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 124])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'diapers', 'Add', 125])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'meat', 'Del', 126])

    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 127])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 128])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Add', 129])
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'beer', 'Del', 130])


    send_channel(program.ip, program.port, "checkout_msg", [addy, addy,1234, 131])
    advance(program)
    send_channel(program.ip, program.port, "action_msg", [addy, addy, 1234, 'papers', 'Add', 132])
    advance(program)    
  end
end
