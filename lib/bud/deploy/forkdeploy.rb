require 'bud/deploy/deployer'
require 'time'

FT_TIMEOUT = 20

module PingLiveness
  state do
    channel :ping_chan, [:@loc, :node_id]
  end
end

module PingClient
  include PingLiveness

  state do
    periodic :ping_clock, 5
  end

  bloom :send_ping do
    ping_chan <~ ping_clock {|c| [@deployer_addr, @node_id]}
  end
end

# An implementation of the Deployer that runs instances using forked local
# processes (listening on an ephemeral port).
#
# Note that this module is included in both the deployer process and in the
# deployed instances. To write code that only runs in one type of process,
# consult the ":deploy" Bud option (which is false in deployed children).
module ForkDeploy
  include Deployer
  include PingLiveness

  state do
    table :last_ping, [:node_id] => [:tstamp]
    scratch :new_ping, last_ping.schema
    scratch :not_live, [:node_id]
    periodic :ft_clock, 2
  end

  bloom :check_liveness do
    # NB: This rule doesn't include nodes that have never sent a ping
    not_live <= (ft_clock * last_ping).pairs do |c, p|
      [p.node_id] if (c.val - FT_TIMEOUT > p.tstamp)
    end
    stdio <~ not_live {|n| ["Dead node: id = #{n.node_id}"]}
  end

  bloom :handle_ping do
    # We assign ping timestamps at the deployer, to avoid sensitivity to
    # node-local clock skew.
    new_ping <= ping_chan {|p| [p.node_id, Time.new]}
    last_ping <+ new_ping
    last_ping <- (new_ping * last_ping).rights(:node_id => :node_id)
  end

  bootstrap do
    return unless @options[:deploy]

    Signal.trap("CHLD") do
      # We receive SIGCHLD when a child process changes state; unfortunately,
      # there's no easy way to tell whether the child process we're getting the
      # signal for is one of ForkDeploy's children. Hence, check if any of the
      # forked children have exited. We also ignore Errno::ECHILD, because
      # someone else's waitpid() could easily race with us.
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

    on_shutdown do
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

    print "Forking local processes"
    @child_pids = []

    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deploy_addr = self.ip_port
    node_count[[]].num.times do |i|
      read, write = IO.pipe
      @child_pids << EventMachine.fork_reactor do
        # XXX: We should shutdown the child's copy of the parent Bud instance
        # (which is inherited across the fork). For now, just reset
        # $bud_instances state.
        Bud.shutdown_all_instances(false)

        # Don't want to inherit our parent's random stuff
        srand

        # Add PingClient to the instance's code
        # XXX: can this be done without instance_eval?
        self.class.instance_eval "include PingClient"
        child = self.class.new(child_opts)
        child.instance_variable_set('@deployer_addr', deploy_addr)
        child.instance_variable_set('@node_id', i)
        child.run_bg
        # Children write their address + port to the pipe
        write.puts child.ip_port
        read.close
        write.close
      end

      # Read child address + port from the pipe
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
