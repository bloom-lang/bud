require 'bud/deploy/deployer'

# An implementation of the Deployer that runs instances using the current Ruby
# process (listening at 127.0.0.1 on an ephemeral port). ThreadDeploy is
# probably not the best name: all the spawned instances are run by a single
# thread, they are just multiplexed via EventMachine.
#
# Note that this module is included in both the deployer process and in the
# deployed instances. To write code that only runs in one type of process,
# consult the ":deploy" Bud option (which is false in deployed children).
module ThreadDeploy
  include Deployer

  def stop_bg
    super
    return unless @options[:deploy]
    @instances.each {|b| b.stop_bg}
  end

  bootstrap do
    return unless @options[:deploy]
    @instances = []
    print "Spawning threads"
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    node_count[[]].num.times do |i|
      b = self.class.new(child_opts)
      b.run_bg
      @instances << b
      node << [i, "localhost:#{b.port}"]
      print "."
    end
    puts "done"
  end
end
