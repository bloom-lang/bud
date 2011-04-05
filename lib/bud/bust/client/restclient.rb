require 'rubygems'
require 'bud'
require 'nestful'
require 'bud/bust/client/idempotence'

module RestClient
  include Idempotence # :nodoc: all

  state do
    # complains about underspecified dataflow because it can't see the
    # nested rules...
    interface input, :rest_req, [:rid, :verb, :form, :url, :params]
    interface output, :rest_response, [:rid, :resp, :exception]

    # we don't really need to store this i suppose
    scratch :rest_req_thread, [:thread]
  end

  bloom :rest_client do
    rest_req_thread <= rest_req.map do |req|
      # start up a new thread to deal with the response
      [Thread.start(req, self) do |r, bud|
         params = (r.params or {})
         begin
           case r.verb
           when :get
             params = params.merge({:format => r.form}) if r.form
             resp_tuple = [r.rid, Nestful.get(r.url, params), false]
           when :post
             # not sure if this is a sensible default for format?
             format = (r.form or :form)
             resp_tuple = [r.rid, Nestful.post(r.url, :format => format,
                                               :params => params), false]
           else
             raise "invalid verb"
           end
         rescue
           resp_tuple = [r.rid, "#{$!}", true]
         end
         # insert the response
         bud.async_do do
           rest_response <+ [resp_tuple]
         end
       end] if bust_idempotent [[req.rid, req.verb, req.form, req.url, req.params,
                                 @budtime]]
    end
  end
end
