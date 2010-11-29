# simple mapreduce
# run "ruby simple_reduce.rb localhost:23456" in one window
# run "ruby simple_reduce.rb localhost:23457" in another

require 'rubygems'
require 'bud'

class SimpleReducer < Bud

  def initialize(ip, port, reducer)
    @reducer = reducer
    super ip, port
  end
  
  def state
    channel     :reducers, ['@addr', 'key', 'value']
    table       :in_channel, ['addr', 'key', 'value']
    scratch     :near_final, ['key'], ['value']
    scratch     :final, ['key'], ['value']
  end

  declare
  def rules
    in_channel <= reducers.map {|t| t}

    near_final <= in_channel.reduce({}) do |memo, t|
      memo[t.key] ||= @reducer.init(t)
      memo[t.key] = @reducer.iter(memo[t.key], t)
      memo
    end

    final <= near_final.map {|t| [t.key, @reducer.final(t.value)]}
  end
end

class Counter
  def init(t)
    0
  end
  
  def iter(curval, t)
    curval+1
  end
  
  def final(finalval)
    finalval
  end
end

source = ARGV[0].split(':')
ip = source[0]
port = source[1].to_i
program = SimpleReducer.new(ip, port, Counter.new)
r = Thread.new {program.run}
sleep 40
program.final.each {|t| puts "[#{t.key}, #{t.value}]"}
