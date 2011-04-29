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
    if node_count[[]]
      print "Spawning threads"
      @instances = []
      node_count[[]].num.times do |i|
        b = self.class.new
        b.run_bg
        @instances << b
        node << [i, "localhost:#{b.port}"]
        print "."
      end
      puts "done"
    end
  end
end
