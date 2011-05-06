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

module ForkDeployProtocol
  state do
    channel :child_ack, [:@loc, :node_id] => [:node_addr]
  end
end

module ForkDeployChild
  include ForkDeployProtocol

  bootstrap do
    child_ack <~ [[@deployer_addr, @node_id, ip_port]]
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
  include ForkDeployProtocol
  include PingLiveness

  state do
    table :ack_buf, [:node_id] => [:node_addr]
    scratch :ack_cnt, [] => [:num]
    scratch :nodes_ready, [] => [:ready]

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
    stdio <~ new_ping {|p| ["Got ping: #{p.inspect}"]}
  end

  bloom :child_info do
    ack_buf <= child_ack {|a| [a.node_id, a.node_addr]}
    ack_cnt <= ack_buf.group(nil, count)
    nodes_ready <= (ack_cnt * node_count).pairs do |nack, ntotal|
      [true] if nack.num == ntotal.num
    end

    node <= (nodes_ready * ack_buf).rights do |a|
      [a.node_id, a.node_addr]
    end
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

    @child_pids = []

    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deployer_addr = self.ip_port
    node_count[[]].num.times do |i|
      @child_pids << Bud.do_fork do
        # XXX: can this be done without instance_eval?
        self.class.instance_eval "include PingClient"
        self.class.instance_eval "include ForkDeployChild"
        child = self.class.new(child_opts)
        child.instance_variable_set('@deployer_addr', deployer_addr)
        child.instance_variable_set('@node_id', i)
        child.run_fg
      end
    end
  end
end
