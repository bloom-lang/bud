require 'rubygems'
require 'bud'
require 'thread'
require 'deployer'

# Starts up a bunch of Bud instances locally on 127.0.0.1, with ephemoral ports.
# This is for the case where you just want to test stuff locally, but you don't
# really care about port numbers.
module LocalDeploy
  include Deployer

  # deal with SIGCHILD
  trap("CLD") {
    pid = Process.wait
    puts "Child pid #{pid}: terminated"
  }

#  def initialize(opt)
#    super
#    if opt[:deploy]
#      deploystrap
#    end
#  end

  def deploystrap
    read, write = IO.pipe
    if node_count[[]] and idempotent [[:node]]
      (0..node_count[[]].num-1).map do |i|
        Process.fork do
          srand # don't want to inherit our parent's random stuff
          foo = self.class.new(:ip => '127.0.0.1') # XXX
          puts "Starting node " + i.to_s + "; pid " + Process.pid.to_s
          foo.run_bg
          # processes write their port to a pipe
          # for some reason, puts isn't atomic?
          write.print foo.port.to_s + "\n"
          EventMachine.reactor_thread.join
        end
      end
      # wait for all to be spun up
      (0..node_count[[]].num-1).map do |i|
        node << [i, "127.0.0.1:" + read.readline.rstrip]
      end
    end
  end

end
