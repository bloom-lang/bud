require 'rubygems'
require 'bud'
require 'json'
require 'socket'
require 'uri'
require 'cgi'

HTTP_VERBS = ["GET", "POST"] #, "DELETE"]

# used this for inspiration:
# http://blogs.msdn.com/b/abhinaba/archive/2005/10/14/474841.aspx
module Bust
  include Bud

  bootstrap do
    Thread.start(self) do |bud|
      BustClass.new(bud)
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
        table_name = uri.path[1..-1]
        success = "HTTP/1.1 200/OK\r\nServer: Bud\r\nContent-type: application/json\r\n\r\n"
        failure = "HTTP/1.1 404/Object Not Found\r\nServer: Bud\r\n\r\n"

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

    def initialize(bud)
      server = TCPServer.new(bud.ip, 8080)

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
