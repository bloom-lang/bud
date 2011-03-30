require 'rubygems'
require 'bud'
require 'restclient'

class TwitterExample
  include Bud
  include RestClient

  bootstrap do
    # get the 20 most recent tweets from twitter
    rest_req <= [[1, :get, :json,
                  'http://api.twitter.com/1/statuses/public_timeline']]
  end

  bloom :print_recent_tweets do
    # print the tweets with user screen names
    stdio <~ rest_response do |r|
      [r.resp.map {|s| s["user"]["screen_name"] + ": " + s["text"]}] if r.rid==1
    end
  end
end

TwitterExample.new.run_fg
