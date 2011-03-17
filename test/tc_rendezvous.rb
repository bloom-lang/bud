require 'rubygems'
require 'bud'
require 'bud/rendezvous'
require 'test/unit'
require 'msgpack'
require 'ruby2ruby'
require 'sourcify'
require 'eventmachine'
require 'socket'
require 'timeout'

module LogicThatTakesTime 
  include BudModule

  state do
    interface input, :start
    channel :one
    scratch :two
    scratch :three
    interface output, :finish
    periodic :timer, 1
  end

  bloom :logos do
    one <~ start
    two <+ one
    three <+ two
    finish <+ three
  end
end

class TL 
  include Bud
  include LogicThatTakesTime
end

class Tests < Test::Unit::TestCase

  # accumulate a set of inputs synchronously
  def test_one
    skel do |tl, i|
      tl.sync_do{ tl.start <+ [["localhost:34343", i]] }
    end
  end

  def test_two
    skel do |tl, i|
      tl.async_do{ tl.start <+ [["localhost:34343", i]] }
    end
  end

  # send 10 inputs to TL.  call blocking receives till we get all 10.
  def skel
    tl = TL.new(:port => 34343)
    tl.run_bg
    glue = Rendezvous.new(tl, tl.finish)
    (0..10).each do |i|
      yield tl, i
    end

    items = []
    success = false
    loop do
      r = glue.block_on(4)
      items << r[1]
      if r[1] == 10
        success = true
        break
      end
    end

    assert(success, "final item not hit")
    assert_equal(11, items.length)
    #tl.t_rules.each{|r| puts "R: #{r.inspect}"}
    glue.stop
    #tl.t_rules.each{|r| puts "R2: #{r.inspect}"}
    tl.stop_bg
  end
end

