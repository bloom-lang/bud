require 'rubygems'
require 'bud'
require 'btquicksort'
require 'bud/deploy/ec2deploy'

# Mixes in quicksort with BinaryTreePartition
class BTQuicksortEC2
  include Bud
  include BTQuicksort
  include EC2Deploy

  # XXX: remove this when we update the AMI's gem
  state do
    channel :ready, [:@loc, :sender]
  end

  # XXX: remove this when we update the AMI's gem
  bootstrap do
    if @options[:deploy]
      #input_list << [[2,5,3,6,0,1,8,7,4,9]]
      input_list << [[9,1,5]]
      node_count <<
        [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
      eval(IO.read('keys.rb'), binding) if File.exists?('keys.rb')
      ruby_command <= [["ruby btquicksort-ec2.rb"]]
      init_dir <= [["."]]
    end

    # XXX: remove this when we update the AMI's gem
    unless @options[:deploy]
      # Send message to the deployer telling 'em we's up.
      File.open("deploy_ip_port", "r") do |f|
        ready <~ [[f.readline.chop, ip_port]]
      end
    end

  end


# XXX: uncomment when we update the AMI's gem
#  deploystrap do
#    input_list << [[2,5,3,6,0,1,8,7,4,9]]
#    node_count <<
#      [2**((Math.log(input_list[[]].list.size)/Math.log(2)).ceil + 1) - 1]
#    eval(IO.read('keys.rb'), binding) if File.exists?('keys.rb')
#    ruby_command <= [["ruby btquicksort-ec2.rb"]]
#    init_dir <= [["."]]
#  end

end

ip, port = ARGV[0].split(':')
# XXX: remove when we update the AMI's gem
ext_ip = ARGV[1]
BTQuicksortEC2.new(:ip => ip,
                   :ext_ip => ext_ip,
                   :port => port,
                   :deploy => ARGV[2]).run_fg

# XXX: uncoment when we update the AMI's gem
#ext_ip, ext_port = ARGV[1].split(':')
#BTQuicksortEC2.new(:ip => ip,
#                   :ext_ip => ext_ip,
#                   :port => port,
#                   :ext_port => ext_port,
#                   :deploy => ARGV[2]).run_f
