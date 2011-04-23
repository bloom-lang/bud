require 'rubygems'
require 'bud'
require 'json'
require 'socket'
require 'uri'
require 'cgi'

HTTP_VERBS = ["GET", "POST"] #, "DELETE"]

# a RESTful interface to Bloom code
module Bust
  include Bud
  attr_reader :bust_port

  # used this for inspiration:
  # http://blogs.msdn.com/b/abhinaba/archive/2005/10/14/474841.aspx

  bootstrap do
    # copied from peter's code; this should probably be in the Bud runtime or in
    # some meta module
    @tables.each do |t|
      t_table_schema << [t[0], t[1].schema.clone]
      t_table_info << [t[0], t[1].class.to_s]
    end

    q = Queue.new
    Thread.start(self) do |bud|
      BustClass.new(bud, q)
    end
    # Wait for socket to be ready before we return from bootstrap.
    r = q.pop
    if r.class <= Exception
      raise r
    else
      @bust_port = r
    end
  end

  class BustClass
    class BustHandler
      def initialize(session, request, body, bud)
        @session = session
        @request = request
        @body = body
        @bud = bud
      end

      def serve()
        puts "Request: " + @request
        puts "Body: " + @body.inspect if @body

        for type in HTTP_VERBS
          if @request =~ Regexp.new(type + " .* HTTP*")
            break reqstr = @request.gsub(Regexp.new(type + " "), '').gsub(/ HTTP.*/, '')
          end
        end

        uri = URI.parse(reqstr)
        uri_params = {}
        uri_params = CGI.parse(uri.query) if uri.query
        table_name = uri.path[1..-1].split(".")[0] # hack; we always return JSON
        # "Access-Control-Allow-Origin: *" disables same-origin policy to allow
        # XMLHttpRequests from any origin
        success = "HTTP/1.1 200 OK\r\nServer: Bud\r\nContent-type: application/json\r\nAccess-Control-Allow-Origin: *\r\n\r\n"
        failure = "HTTP/1.1 404 Object Not Found\r\nServer: Bud\r\nAccess-Control-Allow-Origin: *\r\n\r\n"

        begin
          if @request =~ /GET .* HTTP*/
            puts "GET shouldn't have body" if @body
            # select the appropriate elements from the table
            desired_elements = (eval "@bud." + table_name).find_all do |t|
              uri_params.all? {|k, v| (eval "t." + k.to_s) == v[0]}
            end
            @session.print success
            @session.print desired_elements.to_json
          elsif @request =~ /POST .* HTTP*/
            # instantiate a new tuple
            tuple_to_insert = []
            @body.each do |k, v|
              index = (eval "@bud." + table_name).schema.find_index(k.to_sym)
              for i in (tuple_to_insert.size..index)
                tuple_to_insert << nil
              end
              tuple_to_insert[index] = v[0]
            end
            # actually insert the puppy
            @bud.async_do { (eval "@bud." + table_name) << tuple_to_insert }
            @session.print success
          end
        rescue Exception
          puts "exception: #{$!}"
          @session.print failure
        ensure
          @session.close
        end
      end
    end

    def initialize(bud, q)
      # allow user-configurable port
      begin
        server = TCPServer.new(bud.ip, (bud.options[:bust_port] or 0))
        port = server.addr[1]
        puts "Bust server listening on #{bud.ip}:#{port}"
        # We're now ready to accept connections
        q << port
      rescue Exception => e
        # Avoid deadlock on queue, report exception to caller
        q << e
      end

      loop do
        session = server.accept
        request = session.gets
        length = nil
        body = nil
        while req = session.gets
          length = Integer(req.split(" ")[1]) if req.match(/^Content-Length:/)
          if req.match(/^\r\n$/)
            body = CGI.parse(session.read(length)) if length
            break
          end
        end
        Thread.start(session, request, body, bud) do |session, request, body, bud|
          BustHandler.new(session, request, body, bud).serve()
        end
      end
    end
  end
end
