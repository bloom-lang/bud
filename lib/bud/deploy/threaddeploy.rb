require 'bud/deploy/deployer'

# An implementation of the Deployer that runs instances using Ruby threads
# (listening at 127.0.0.1 on an ephemeral port).
module ThreadDeploy
  include Deployer

  def stop_bg
    super

    if @options[:deploy]
      @instances.each {|b| b.stop_bg}
    end
  end

  deploystrap do
    out_io = @options[:stdout]
    out_io ||= $stdout
    out_io.print "Spawning threads"
    @instances = []
    child_opts = @options[:deploy_child_opts]
    child_opts ||= {}
    node_count[[]].num.times do |i|
      b = self.class.new(child_opts)
      b.run_bg
      @instances << b
      node << [i, "localhost:#{b.port}"]
      out_io.print "."
    end
    out_io.puts "done"
  end
end
