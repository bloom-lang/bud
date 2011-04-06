require 'rubygems'
require 'bud'
require 'tokenring'
require 'bud/deploy/ec2deploy'

# Mixes in quicksort with BinaryTreePartition
class RingLocal
  include Bud
  include TokenRing
  include EC2Deploy

  # XXX: remove this when we update the AMI's gem
  state do
    channel :ready, [:@loc, :sender]
  end

  #deploystrap do
  #  node_count << [10]
  #end

  # XXX: remove this when we update the AMI's gem
  bootstrap do
    if @options [:deploy]
      node_count << [10]
      eval(IO.read('keys.rb'), binding) if File.exists?('keys.rb')
      ruby_command << ["ruby tokenring-ec2.rb"]
      init_dir << ["."]
    end

    # XXX: remove this when we update the AMI's gem
    unless @options[:deploy]
      # Send message to the deployer telling 'em we's up.
      File.open("deploy_ip_port", "r") do |f|
        ready <~ [[f.readline.chop, ip_port]]
      end
    end
  end

end

ip, port = ARGV[0].split(':')
ext_ip = ARGV[1]
RingLocal.new(:ip => ip,
              :ext_ip => ext_ip,
              :port => port,
              :deploy => ARGV[2]).run_fg
