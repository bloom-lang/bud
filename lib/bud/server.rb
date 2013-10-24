require 'socket'

class Bud::BudServer < EM::Connection #:nodoc: all
  def initialize(bud, channel_filter)
    @bud = bud
    @channel_filter = channel_filter
    @filter_buf = {}
    @pac = MessagePack::Unpacker.new
    super
  end

  def receive_data(data)
    obj = Marshal.load(data)
    recv_message(obj)

    # apply the channel filter to each channel's pending tuples
    buf_leftover = {}
    @filter_buf.each do |tbl_name, buf|
      if @channel_filter
        accepted, saved = @channel_filter.call(tbl_name, buf)
      else
        accepted = buf
        saved = []
      end

      unless accepted.empty?
        @bud.inbound[tbl_name] ||= []
        @bud.inbound[tbl_name].concat(accepted)
      end
      buf_leftover[tbl_name] = saved unless saved.empty?
    end
    @filter_buf = buf_leftover

    begin
      @bud.tick_internal if @bud.running_async
    rescue Exception => e
      # If we raise an exception here, EM dies, which causes problems (e.g.,
      # other Bud instances in the same process will crash). Ignoring the
      # error isn't best though -- we should do better (#74).
      puts "Exception handling network messages: #{e}"
      puts e.backtrace
      puts "Inbound messages:"
      @bud.inbound.each do |chn_name, t|
        tuples = t.map(&:first)
        puts "    #{tuples.inspect} (channel: #{chn_name})"
      end
      puts "Periodics:"
      @bud.periodic_inbound.each do |tbl_name, t|
        tuples = t.map(&:first)
        puts "    #{tuples.inspect} (periodic: #{tbl_name})"
      end
      @bud.inbound.clear
      @bud.periodic_inbound.clear
    end

    @bud.rtracer.sleep if @bud.options[:rtrace]
  end

  def recv_message(obj)
    unless (obj.class <= Array and obj.length == 2 and
            @bud.tables.include?(obj[0]) and obj[1].class <= Enumerable)
      raise Bud::Error, "bad inbound message of class #{obj.class}: #{obj.inspect}"
    end

    tbl_name, vals = obj

    # Check for range compressed message
    if vals.kind_of? Array and vals.length == 2 and
       vals[0].kind_of? Hash and vals[1].kind_of? Integer
      groups, range_idx = vals
      vals = groups.flat_map do |k,v|
        v.map do |i|
          t = k.dup
          t.insert(range_idx, i)
        end
      end
    end

    port, ip = Socket.unpack_sockaddr_in(get_peername)
    vals.each do |tuple|
      obj = [tbl_name, [tuple, "#{ip}:#{port}"]]
      @bud.rtracer.recv(obj) if @bud.options[:rtrace]
      @filter_buf[obj[0].to_sym] ||= []
      @filter_buf[obj[0].to_sym] << obj[1]
    end
  end
end
