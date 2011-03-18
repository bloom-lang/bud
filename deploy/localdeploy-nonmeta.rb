require 'rubygems'
require 'bud'
require 'thread'
require 'countatomicdelivery'

# Starts up a bunch of Bud instances locally on 127.0.0.1, with ephemoral ports.
# This is for the case where you just want to test stuff locally, but you don't
# really care about port numbers.
module LocalDeploy
  include CountAtomicDelivery

  state do
    table :node, [:uid] => [:node]
    table :node_count, [] => [:num]
    table :initial_data, [:uid, :pred, :data]
    table :dead, [:dead]
    channel :dont_care, [:@loc] # XXX: hack to get around laziness
  end

  # deal with SIGCHILD
  trap("CLD") {
    pid = Process.wait
    puts "Child pid #{pid}: terminated"
  }

  def initialize(opt)
    super
    if opt[:deploy]
      deploystrap
    end

    if opt[:nat]
      @my_ip = open("http://myip.dk") do |f|
        /([0-9]{1,3}\.){3}[0-9]{1,3}/.match(f.read)[0].to_a[0]
      end
    else
      @my_ip = @ip
    end
  end

  def deploystrap
    read, write = IO.pipe
    if node_count[[]] and idempotent [[:node]]
      (0..node_count[[]].num-1).map do |i|
        Process.fork do
          srand # don't want to inherit our parent's random stuff
          foo = self.class.new(:ip => '127.0.0.1')
          puts "Starting node " + i.to_s + "; pid " + Process.pid.to_s
          foo.run_bg
          # processes write their port to a pipe
          # for some reason, puts isn't atomic?
          write.print foo.port.to_s + "\n"
          EventMachine.reactor_thread.join
        end
      end
      # wait for all to be spun up
      (0..node_count[[]].num-1).map do |i|
        node << [i, "127.0.0.1:" + read.readline.rstrip]
      end
    end
  end

  def me() @my_ip + ":" + @port.to_s end

  def idempotent(r) (dead.include? r) ? false : dead.insert(r) end

  # distribute the EDB to each node
  #
  # XXX: this may break coordination protocols that assume the EDB is present
  # before any messages are received.  In order to fix this, we would probably
  # need to globally synchronize to ensure that "timestamp 0" gets "fully
  # evaluated" before any messages can be sent
  bloom :distribute_data do
    atomic_data_in <= join([node, initial_data],
                           [node.uid, initial_data.uid]).map do |n, i|
      [n.node, [i.pred, i.data]] if idempotent [[n.node, i.pred, i.data]]
    end


    # add tuples all at once
    dont_care <~ atomic_data_out.map do |a|
      if idempotent a
        a.tuple[1].map do |d|
          eval a.tuple[0].to_s + " <+ [" + d.inspect + "]"
        end
        [ip_port]
      end
    end

  end
end
