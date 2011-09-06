require 'bud/deploy/deployer'

# An implementation of the Deployer that runs instances using the current Ruby
# process (listening on an ephemeral port). ThreadDeploy is probably not the
# best name: all the spawned instances are run by a single thread, they are just
# multiplexed via EventMachine.
#
# Note that this module is included in both the deployer process and in the
# deployed instances. To write code that only runs in one type of process,
# consult the ":deploy" Bud option (which is false in deployed children).
module ThreadDeploy
  include Deployer

  bootstrap do
    return unless @options[:deploy]

    @instances = []
    on_shutdown do
      @instances.each {|b| b.stop}
    end

    print "Spawning threads"
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    deployer_addr = self.ip_port
    node_count[[]].num.times do |i|
      b = self.class.new(child_opts)
      b.instance_variable_set('@deployer_addr', deployer_addr)
      b.instance_variable_set('@node_id', i)
      b.run_bg
      @instances << b
      node << [i, b.ip_port]
      node_ready << [i]
      print "."
    end
    puts "done"
  end
end
