require 'rubygems'
require 'bud'

class PriorityQ < Bud
  def state
    table :q, [:item] => [:priority]
    scratch :out, [:item] => [:priority]
    scratch :minny, [:priority]
    scratch :out2, [:item] => [:priority]
  end

  def bootstrap
    q << ['c', 2]
    q << ['d', 3]
    q << ['a', 1]
    q << ['b', 2]
  end

  declare
  def logic
    out <= q.argagg(:min, [], q.priority)
    minny <= q.group(nil, min(q.priority))
    q <- out
    out2 <= natjoin([q, minny]).map{|q, m| q+m}
  end
end

program = PriorityQ.new

(1..4).each do
  puts '---tick---'
  program.tick
  program.out.each {|o| puts "argmin: #{o.inspect}"}
  program.out2.each {|o| puts "joinmin: #{o.inspect}"}
end
