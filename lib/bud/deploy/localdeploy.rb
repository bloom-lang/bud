require 'rubygems'
require 'bud'
require 'bud/deploy/deployer'

# Starts up a bunch of Bud instances locally on 127.0.0.1, with ephemeral ports.
module LocalDeploy
  include Deployer

  def stop_bg
    super

    # Setting the SIGCHLD handler to "IGNORE" actually results in waitpid()
    # being called automatically (to avoid zombies), at least on OSX. This is
    # not what we want, since it would cause a subsequent waitpid() to fail.
    # Instead, use an empty block.
    trap("CHLD") {}
    @dead_pids ||= []
    pids = @child_pids - @dead_pids
    pids.each do |p|
      Process.kill("TERM", p)
      Process.waitpid(p)
     end
    trap("CHLD", @old_handler)
  end

  deploystrap do
    @old_handler = trap("CHLD") do
      # We get a SIGCHLD every time a child process changes state and there's no
      # easy way to tell whether the child we're getting the signal for is one
      # of local deploy's children. Hence, we spin on waitpid() until there are
      # no more children to cleanup. We also ignore Errno::ECHILD, because
      # someone else's waitpid() could easily race with us.
      loop do
        begin
          pid = Process.waitpid(0, Process::WNOHANG)
          break if pid.nil?
          @dead_pids ||= []
          @dead_pids << pid
        rescue Errno::ECHILD
          break
        end
      end
    end

    read, write = IO.pipe
    if node_count[[]]
      print "Forking local processes"
      @child_pids = []
      (0..node_count[[]].num-1).map do |i|
        @child_pids << Process.fork do
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
