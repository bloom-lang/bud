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

      $stdout = StringIO.new

      assert_nothing_raised do
        server = ReblServer.new.run_bg
        client = ReblClient.new
        result = client.sync_callback(:rest_req, [[1, :post, :form,
                                                   "http://localhost:8080/foo",
                                                   {:bar => 'a', :baz => 'b',
                                                     :qux => 'c'}]],
                                      :rest_response)
      end
      assert_equal(result.first[0], 1)
      assert_equal(result.first[2], false)

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[2, :post, :form,
                                                   "http://localhost:8080/foo",
                                                   {:qux => 'd', :bar => 'a',
                                                     :baz => 'b'}]],
                                      :rest_response)
      end
      assert_equal(result.first[0], 2)
      assert_equal(result.first[2], false)

      assert_nothing_raised do
        result = client.sync_callback(:rest_req, [[3, :get, :json,
                                                 "http://localhost:8080/foo",
                                                 {:qux => 'c'}]],
                                    :rest_response)
      end
      assert_equal(result.first[0], 3)
      assert_equal(result.first[1], [['a', 'b', 'c']])
      assert_equal(result.first[2], false)
    ensure
      $stdout = STDOUT
    end
  end
end
