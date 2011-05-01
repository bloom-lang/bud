require 'bud/deploy/deployer'

# An implementation of the Deployer that runs instances using forked local
# processes (listening at 127.0.0.1 on an ephemeral port).
#
# Note that this module is included in both the deployer process and in the
# deployed instances. To write code that only runs in one type of process,
# consult the ":deploy" Bud option (which is false in deployed children).
module ForkDeploy
  include Deployer

  def stop_bg
    super
    return unless @options[:deploy]

    # NB: Setting the SIGCHLD handler to "IGNORE" results in waitpid() being
    # called automatically (to cleanup zombies), at least on OSX. This is not
    # what we want, since it would cause a subsequent waitpid() to fail.
    Signal.trap("CHLD", "DEFAULT")
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

  bootstrap do
    return unless @options[:deploy]

    Signal.trap("CHLD") do
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

    print "Forking local processes"
    @child_pids = []
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    node_count[[]].num.times do |i|
      read, write = IO.pipe
      @child_pids << EventMachine.fork_reactor do
        # Don't want to inherit our parent's random stuff.
        srand
        child = self.class.new(child_opts)
        child.run_bg
        # Processes write address/port to the pipe
        write.puts child.ip_port
      end

      # Read child address/port from the pipe.
      addr = read.readline.rstrip
      node << [i, addr]
      read.close
      write.close
      print "."
      $stdout.flush
    end

    puts "done"
  end
end
