require './test_common'
require 'bud/rebl'
require 'bud/bust/client/restclient'
require 'bud/bust/bust'
require 'stringio'

class ReblServer
  include Bud
  include Bust

  state do
    table :foo, [:bar, :baz, :qux]
    scratch :bax, [:bar, :baz, :qux]
  end
end

class ReblClient
  include Bud
  include RestClient
end

class TestBust < MiniTest::Unit::TestCase
  def test_bust
    # run_bg blocks on bootstrap succeeding, and bootstrap blocks on BUST socket
    # being opened, so this should be race-free.
    begin
      result = nil
      server = nil
      client = nil
      host = nil

      $stdout = StringIO.new

      server = ReblServer.new
      server.run_bg
      client = ReblClient.new
      client.run_bg
      host = "http://localhost:#{server.bust_port}"
      result = client.sync_callback(:rest_req, [[1, :post, :form,
                                                 "#{host}/foo",
                                                 {:bar => 'a', :baz => 'b',
                                                   :qux => 'c'}]],
                                    :rest_response)
      msg = result[0] # the first response
      assert_equal(msg.rid, 1)
      assert_equal(msg.exception, false)

      result = client.sync_callback(:rest_req, [[2, :post, :form,
                                                 "#{host}/foo",
                                                 {:qux => 'd', :bar => 'a',
                                                   :baz => 'b'}]],
                                    :rest_response)
      msg = result[0] # the first response
      assert_equal(msg.rid, 2)
      assert_equal(msg.exception, false)

      result = client.sync_callback(:rest_req, [[3, :get, :json,
                                                 "#{host}/foo",
                                                 {:qux => 'c'}]],
                                    :rest_response)
      msg = result[0] # the first response
      assert_equal(msg.rid, 3)
      assert_equal(msg.resp, [['a', 'b', 'c']])
      assert_equal(msg.exception, false)

      result = client.sync_callback(:rest_req, [[4, :post, :form,
                                                 "#{host}/bax",
                                                 {:qux => 'd', :bar => 'a',
                                                   :baz => 'b'}]],
                                    :rest_response)
      msg = result[0] # the first response
      assert_equal(msg.rid, 4)
      assert_equal(msg.exception, false)

      result = client.sync_callback(:rest_req, [[5, :get, :json,
                                                 "#{host}/bax",
                                                 {:qux => 'd'}]],
                                    :rest_response)
      msg = result[0] # the first response
      assert_equal(msg.rid, 5)
      assert_equal(msg.exception, false)
      assert_equal(msg.resp, [['a', 'b', 'd']])
    ensure
      $stdout = STDOUT
    end
  end
end
