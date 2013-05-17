begin
  require 'zookeeper'
  Bud::HAVE_ZOOKEEPER = true
rescue LoadError
end

module Bud
  # Persistent table implementation based on Zookeeper.
  class BudZkTable < BudPersistentCollection # :nodoc: all
    def initialize(name, zk_path, zk_addr, bud_instance)
      unless defined? Bud::HAVE_ZOOKEEPER
        raise Bud::Error, "zookeeper gem is not installed: zookeeper-backed stores cannot be used"
      end

      super(name, bud_instance, [:key] => [:val, :opts])

      @zk = Zookeeper.new(zk_addr)
      zk_path = zk_path.chomp("/") unless zk_path == "/"
      @zk_path = zk_path
      @base_path = @zk_path
      @base_path += "/" unless @zk_path.end_with? "/"
      @store_mutex = Mutex.new
      @zk_mutex = Mutex.new
      @next_storage = {}
      @saw_delta = false
      @child_watch_id = nil
    end

    def invalidate_at_tick
      true
    end

    def invalidate_cache
    end

    # Since the watcher callbacks might invoke EventMachine, we wait until after
    # EM startup to start watching for Zk events.
    def start_watchers
      # Watcher callbacks are invoked in a separate Ruby thread. Note that there
      # is a possible deadlock between invoking watcher callbacks and calling
      # close(): if we get a watcher event and a close at around the same time,
      # the close might fire first. Closing the Zk handle will block on
      # dispatching outstanding watchers, but it does so holding the @zk_mutex,
      # causing a deadlock. Hence, we just have the watcher callback spin on the
      # @zk_mutex, aborting if the handle is ever closed.
      @child_watcher = Zookeeper::Callbacks::WatcherCallback.new do
        while true
          break if @zk.closed?
          if @zk_mutex.try_lock
            get_and_watch unless @zk.closed?
            @zk_mutex.unlock
            break
          end
        end
      end

      @stat_watcher = Zookeeper::Callbacks::WatcherCallback.new do
        while true
          break if @zk.closed?
          if @zk_mutex.try_lock
            stat_and_watch unless @zk.closed?
            @zk_mutex.unlock
            break
          end
        end
      end

      stat_and_watch
    end

    def stat_and_watch
      r = @zk.stat(:path => @zk_path, :watcher => @stat_watcher)

      unless r[:stat].exists
        # The given @zk_path doesn't exist, so try to create it. Unclear
        # whether this is always the best behavior.
        r = @zk.create(:path => @zk_path)
        if r[:rc] != Zookeeper::ZOK and r[:rc] != Zookeeper::ZNODEEXISTS
          raise
        end
      end

      # Make sure we're watching for children
      get_and_watch unless @child_watch_id
    end

    def get_and_watch
      r = @zk.get_children(:path => @zk_path, :watcher => @child_watcher)
      return unless r[:stat].exists
      @child_watch_id = r[:req_id]

      # XXX: can we easily get snapshot isolation?
      new_children = {}
      r[:children].each do |c|
        child_path = @base_path + c

        get_r = @zk.get(:path => child_path)
        unless get_r[:stat].exists
          puts "ZK: failed to fetch child: #{child_path}"
          return
        end

        data = get_r[:data]
        # XXX: For now, conflate empty string values with nil values
        data ||= ""
        new_children[c] = [c, data]
      end

      # We successfully fetched all the children of @zk_path; arrange to install
      # the new data into @storage at the next Bud tick
      need_tick = false
      @store_mutex.synchronize {
        @next_storage = new_children
        if @storage != @next_storage
          need_tick = true
          @saw_delta = true
        end
      }

      # If we have new data, force a new Bud tick in the near future
      if need_tick and @bud_instance.running_async
        EventMachine::schedule {
          @bud_instance.tick_internal
        }
      end
    end

    def tick
      @store_mutex.synchronize {
        return unless @saw_delta
        @storage = @next_storage
        @next_storage = {}
        @saw_delta = false
      }
    end

    def flush
      each_from([@pending]) do |t|
        path = @base_path + t.key
        data = t.val
        ephemeral = false
        sequence = false

        opts = t.opts
        unless opts.nil?
          if opts[:ephemeral] == true
            ephemeral = true
          end
          if opts[:sequence] == true
            sequence = true
          end
        end

        r = @zk.create(:path => path, :data => data,
                       :ephemeral => ephemeral, :sequence => sequence)
        if r[:rc] == Zookeeper::ZNODEEXISTS
          puts "Ignoring duplicate insert: #{t.inspect}"
        elsif r[:rc] != Zookeeper::ZOK
          puts "Failed create of #{path}: #{r.inspect}"
        end
      end
      @pending.clear
    end

    def close
      # See notes in start_watchers.
      @zk_mutex.synchronize { @zk.close }
    end

    superator "<~" do |o|
      pending_merge(o)
    end

    superator "<+" do |o|
      raise Bud::Error, "illegal use of <+ with zookeeper store '#{@tabname}' on left"
    end

    def <=(o)
      raise Bud::Error, "illegal use of <= with zookeeper store '#{@tabname}' on left"
    end

    def <<(o)
      raise Bud::Error, "illegal use of << with zookeeper store '#{@tabname}' on left"
    end
  end
end
