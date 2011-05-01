require 'rubygems'
require 'bud'
require 'bud/deploy/ec2deploy'
require 'tokenring'

class RingEC2
  include Bud
  include TokenRing
  include EC2Deploy

  deploystrap do
    raise "keys.rb must exist in the current directory" unless File.exists? "keys.rb"
    eval(IO.read('keys.rb'), binding)
    node_count << [10]
    ruby_command << ["ruby tokenring-ec2.rb"]
    init_dir << ["."]
  end
end

ip, port = ARGV[0].split(':')
ext_ip, ext_port = ARGV[1].split(':')
RingEC2.new(:ip => ip,
            :port => port,
            :ext_ip => ext_ip,
            :ext_port => ext_port,
            :deploy => ARGV[2]).run_fg
