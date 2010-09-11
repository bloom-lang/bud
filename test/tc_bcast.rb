require 'rubygems'
require 'bud'
require 'test/unit'

#require 'lib/fifo_broadcast'
require 'lib/rb2'


class TestBcast < Test::Unit::TestCase

  def test_simple
    program = SimpleBroadcast.new('127.0.0.1', 10001)
    # note that I must tick once so that "member" is available
    program.tick

    program.member << ['127.0.0.1:10001']
    program.member << ['127.0.0.1:10002']
    program.member << ['127.0.0.1:10003']
    program.message <+ [["foo"]] 
    assert_nothing_raised(RuntimeError) {program.run_bg}
    assert_nothing_raised(RuntimeError) {program.tickle}
    sleep 1
    program.deliver.each do |d| 
      assert_equal("foo", d.message)
    end

  end

  def test_reliable
    p = ReliableBroadcast.new('127.0.0.1', 10002)
    p.tick
    p.member << ['127.0.0.1:10001']
    p.member << ['127.0.0.1:10002']
    p.member << ['127.0.0.1:10003']
    p.message <+ [["foo"]] 

    assert_nothing_raised(RuntimeError) {p.run_bg}
    assert_nothing_raised(RuntimeError) {p.tickle}
    sleep 1
    p.deliver.each do |d| 
      assert_equal("foo", d.message)
    end
  end

  def ntest_fifo
    p = FifoBroadcast.new('127.0.0.1', 10002)
    p.tick
    p.member << ['127.0.0.1:10001']
    p.member << ['127.0.0.1:10002']
    p.member << ['127.0.0.1:10003']
    p.fmessage <+ [["foo"]] 

    assert_nothing_raised(RuntimeError) {p.run_bg}
    assert_nothing_raised(RuntimeError) {p.tickle}
    p.fmessage <+ [["bar"]] 
    assert_nothing_raised(RuntimeError) {p.tickle}
    sleep 1
    assert_nothing_raised(RuntimeError) {p.tickle}
    sleep 1
    p.fdeliver.each do |d| 
      print "message, #{d.message}\n"
      assert_equal("foo", d.message[1])
    end
  end 

end
