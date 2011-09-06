require 'test_common'
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

class TestBust < Test::Unit::TestCase
  def test_bust
    # run_bg blocks on bootstrap succeeding, and bootstrap blocks on BUST socket
    # being opened, so this should be race-free.
    begin
      result = nil
      server = nil
      client = nil
      host = nil

      $stdout = StringIO.new

      assert_nothing_raised do
        server = ReblServer.new
        server.run_bg
        client = ReblClient.new
        host = "http://localhost:#{server.bust_port}"
        result = client.sync_callback(:rest_req, [[1, :post, :form,
                                                   "#{host}/foo",
                                                   {:bar => 'a', :baz => 'b',
                                                     :qux => 'c'}]],
                                      :rest_response)
      end
      assert_equal(1, result.first[0])
      assert_equal(false, result.first[2])

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[2, :post, :form,
                                                   "#{host}/foo",
                                                   {:qux => 'd', :bar => 'a',
                                                     :baz => 'b'}]],
                                      :rest_response)
      end
      assert_equal(2, result.first[0])
      assert_equal(false, result.first[2])

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[3, :get, :json,
                                                   "#{host}/foo",
                                                   {:qux => 'c'}]],
                                      :rest_response)
      end
      assert_equal(3, result.first[0])
      assert_equal([['a', 'b', 'c']], result.first[1])
      assert_equal(false, result.first[2])

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[4, :post, :form,
                                                   "#{host}/bax",
                                                   {:qux => 'd', :bar => 'a',
                                                     :baz => 'b'}]],
                                      :rest_response)
      end
      assert_equal(4, result.first[0])
      assert_equal(false, result.first[2])

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[5, :get, :json,
                                                   "#{host}/bax",
                                                   {:qux => 'd'}]],
                                      :rest_response)
      end
      assert_equal(5, result.first[0])
      assert_equal([['a', 'b', 'd']], result.first[1])
      assert_equal(false, result.first[2])
    ensure
      $stdout = STDOUT
    end
  end
end
