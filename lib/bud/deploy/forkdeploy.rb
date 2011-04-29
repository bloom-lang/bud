require 'bud/deploy/deployer'

# An implementation of the Deployer that runs instances using forked local
# processes (listening at 127.0.0.1 on an ephemeral port).
module ForkDeploy
  include Deployer

  def stop_bg
    super

    # NB: Setting the SIGCHLD handler to "IGNORE" results in waitpid() being
    # called automatically (to cleanup zombies), at least on OSX. This is not
    # what we want, since it would cause a subsequent waitpid() to fail.
    trap("CHLD", "DEFAULT")
    @dead_pids ||= []
    pids = @child_pids - @dead_pids
    pids.each do |p|
      begin
        Process.kill("TERM", p)
        Process.waitpid(p)
      rescue Errno::ESRCH
      end
     end
  end

  deploystrap do
    trap("CHLD") do
      # We get a SIGCHLD every time a child process changes state and there's no
      # easy way to tell whether the child process we're getting the signal for
      # is one of ForkDeploy's children. Hence, check if any of the forked
      # children have exited. We also ignore Errno::ECHILD, because someone
      # else's waitpid() could easily race with us.
      @child_pids.each do |c|
        begin
          pid = Process.waitpid(c, Process::WNOHANG)
          unless pid.nil?
            @dead_pids ||= []
            @dead_pids << pid
          end
        rescue Errno::ECHILD
        end
      end
    end

    read, write = IO.pipe
    if node_count[[]]
      print "Forking local processes"
      @child_pids = []
      node_count[[]].num.times do
        @child_pids << EventMachine.fork_reactor do
          # Don't want to inherit our parent's random stuff.
          srand
          child = self.class.new
          child.run_bg
          print "."
          $stdout.flush
          # Processes write their port to the pipe.
          write.print "#{child.port}\n"
        end
      end

      # Read ports from pipe.
      node_count[[]].num.times do |i|
        node << [i, "localhost:" + read.readline.rstrip]
      end
      read.close
      write.close
      puts "done"
    end
  end
end
