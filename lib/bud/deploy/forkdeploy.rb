require 'bud/deploy/deployer'

FT_TIMEOUT = 10

module PingLiveness
  state do
    channel :ping_chan, [:@loc, :node_id]
  end
end

module PingClient
  include PingLiveness

  state do
    periodic :ping_clock, 2
  end

  bloom :send_ping do
    ping_chan <~ ping_clock {|c| [@deployer_addr, @node_id]}
  end
end

# An implementation of the Deployer that runs instances using forked local
# processes (listening at 127.0.0.1 on an ephemeral port).
#
# Note that this module is included in both the deployer process and in the
# deployed instances. To write code that only runs in one type of process,
# consult the ":deploy" Bud option (which is false in deployed children).
module ForkDeploy
  include Deployer
  include PingLiveness

  state do
    table :last_ping, [:node_id] => [:tstamp]
    periodic :ft_clock, 2
  end

  bloom :check_liveness do
    temp :not_live <= (ft_clock * last_ping).pairs do |c, p|
      [p.node_id] if (c.val - FT_TIMEOUT < p.tstamp)
    end
  end

  bloom :handle_ping do
    temp :new_ping <= ping_chan {|p| [p.node_id, Time.new]}
    last_ping <+ new_ping
    last_ping <- (new_ping * last_ping).rights(:node_id => :node_id)
  end

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

    read, write = IO.pipe
    print "Forking local processes"
    @child_pids = []

    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deploy_addr = self.ip_port
    node_count[[]].num.times do |i|
      @child_pids << EventMachine.fork_reactor do
        # Don't want to inherit our parent's random stuff.
        srand

        # Add PingClient to the instance's code
        # XXX: can this be done without instance_eval?
        self.class.instance_eval "include PingClient"
        child = self.class.new(child_opts)
        child.instance_variable_set('@deployer_addr', deploy_addr)
        child.instance_variable_set('@node_id', i)
        child.run_bg
        print "."
        $stdout.flush
        # Processes write their port to the pipe.
        write.puts "#{child.port}"
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
