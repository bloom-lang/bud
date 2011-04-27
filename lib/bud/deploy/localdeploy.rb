require 'rubygems'
require 'bud'
require 'thread'
require 'bud/deploy/deployer'

# Starts up a bunch of Bud instances locally on 127.0.0.1, with ephemeral ports.
module LocalDeploy
  include Deployer

  def stop_bg
    super
    for p in @pids
      Process.kill("INT", p)
    end
    Process.waitall
    trap("CHLD", @old_handler)
  end

  deploystrap do
    @old_handler = trap("CHLD") do
      pid = Process.wait
      puts "Child pid #{pid}: terminated"
    end

    read, write = IO.pipe
    if node_count[[]]
      print "Forking local processes"
      @pids = []
      (0..node_count[[]].num-1).map do |i|
        @pids << Process.fork do
          # Don't want to inherit our parent's random stuff.
          srand
          child = self.class.new
          child.run_bg
          print "."
          $stdout.flush
          # Processes write their port to the pipe.
          write.print "#{child.port}\n"
          EventMachine.reactor_thread.join
        end
      end

      # Read ports from pipe.
      (0..node_count[[]].num-1).map do |i|
        node << [i, "localhost:" + read.readline.rstrip]
      end
      read.close
      write.close
      puts "done"
    end
  end
end
