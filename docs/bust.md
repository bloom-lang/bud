BUST stands for BUd State Transfer and it is a REST interface to BUD.  BUST consists of a Bud implementation of a client and server.  The client implements bindings to a subset of the Ruby Nestful library, and the server is a lightweight HTTP server written in Ruby.  Note that the BUST server currently sets the "Access-Control-Allow-Origin: *" HTTP header to override web browsers' same-origin policy.

Right now BUST supports "GET" and "POST" requests, and may support "DELETE" and "PUT" requests in the future.

# BUST Server

For the BUST server, a "GET" request corresponds to retrieving a subset of rows of a table, and a "POST" request corresponds to inserting a row into a table.  For example, the following "GET" request (assuming BUST is running on port 8080):

GET localhost:8080/foo?bar=hello&baz=world

would retrieve all rows in table "foo" where named schema attribute "bar" is equal to the string "hello", and named schema attribute "baz" is equal to the string "world".  Right now, one limitation of BUST is that only strings are supported.

To use BUST in your program, ensure you have the json gem installed.  Add the "require" line for BUST:

    require "bust/bust"

In your class, make sure to:

    include Bust

That's it!  Now a BUST server will be started up when your class is instantiated.  By default, this server will listen on port 8080, but you can change this by passing a port via the "bust_port" option when you instantiate your class.

You can test out the BUST server using Ruby's "net/http" library if you want, and you can also check out "BUST Inspector", a sample AJAX application that allows you to view the state of a bud instance.

## net/http Example

Try running "bustexample.rb" in the "bust/" directory:

    cd bust/
    ruby bustexample.rb

Now, let's interact with our example using "net/http" from within IRB.  Start up an IRB instance:

    irb
    irb(main):001:0> require 'net/http'
    => true

bustexample.rb defines a single relation called "foo":

    table :foo, [:bar, :baz, :qux]

Let's fire off some requests.  First, let's put a new foo fact in:

    irb(main):002:0> res = Net::HTTP.post_form(URI.parse('http://localhost:8080/foo'), {:bar => "a", :baz => "b", :qux => "c"})
    => #<Net::HTTPOK 200 /OK readbody=true>

Now, let's retrieve all foo facts where the "qux" attribute is "c", and the "baz" attribute is "b":

    irb(main):003:0> res = Net::HTTP.get(URI.parse('http://localhost:8080/foo?qux=c&baz=b'))
    => "[[\"a\",\"b\",\"c\"]]"

Note that the response is a JSON array.


## BUST Inspector ==

BUST Inspector -- an example app that uses XMLHttpRequests to inspect state in a Bud program using BUST is included -- (bust/bustinspector.html).  Right now, it assumes that the Bud instance you're trying to inspect is listening on "localhost" at port "8080".  BUST Inspector is tested to work in Firefox, and may or may not work in other browsers.  BUST Inspector will query your Bud instance every second for metadata describing the tables and their schema.  It will display a list of the tables in a pane on the left of the screen, with a checkbox next to each table.  Selecting a checkbox renders the current table contents in the right pane (these are also updated every second while the box is checked).


# BUST Client

The BUST client (located in the "bust/client" folder) allows Bud applications to access REST services (including a Bud client hosting a BUST instance). The REST client is basically a wrapper for the Ruby nestful library. You'll need to ensure you have the "nestful" gem installed before you can use the REST client. To use it in your application, you need to put the require line:

    require 'bust/client/restclient'

and the include line:

    include RestClient

To make requests, insert into the rest_req interface, whose defintion is reproduced below:

    interface input, :rest_req, [:rid, :verb, :form, :url, :params]

"rid" is a unique ID for the request, "verb" is one of ":get" or ":post", "form" is the format of the request, for example, you might use ":json", or if you're doing a form post, you'd use "form". If set to nil, "form" defaults to ":form" for ":post", and is omitted from a ":get". For ":get" requests, the "form" parameter seems to be appended onto the end of "url". For example, if you do a ":get" for "http://example.com/ex" with "form" set to ":json", the library sends an HTTP GET to "http://example.com/ex.json". "params" is a hash, which comprises the query string for a ":get", and the contents of the body in a ":post" with "form" set to ":form".

The output interface is:

    interface output, :rest_response, [:rid, :resp, :exception]

"rid" is the unique ID supplied when the request was made, "resp" is the parsed response from the server. For example, if you do a ":json" ":get", then "resp" will contain whatever JSON object was returned converted into a Ruby object, e.g., array, hash, etc. If there is an exception, then "resp" will contain a string describing the exception, and "exception" will be set to true; otherwise, "exception" will be set to false.

A simple example is included (twitterexample.rb) that does an HTTP GET on Twitter's public timeline, returning the most recent statuses, and prints them to stdout.

The BUST client does not yet support OAuth. Also unsupported so far is HTTP DELETE and PUT.