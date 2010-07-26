# simple shortest paths
# note use of program.tick at bottom to run a single timestemp 
# and inspect "shortest" relation
require 'rubygems'
require 'bud'

class PriorityQ < Bud
  def initialize(ip, port)
    super(ip,port)
  end
  
  def state
    table :q, ['item'], ['priority']
    scratch :out, ['item'], ['priority']
    scratch :minny, ['priority']
    scratch :out2, ['item'], ['priority']
  end
  
  def declaration
    strata[0] = rules {
      q << ['c', 2] if budtime == 1
      q << ['d', 3] if budtime == 1
      q << ['a', 1] if budtime == 1
      q << ['b', 2] if budtime == 1
    }
    
    strata[1] = rules {
      out <= q.argagg(:min, [], q.priority)
      minny <= q.group([], min(q.priority))
      q <- out.map{|t| t}
    }
    
    strata[2] = rules {
      out2 <= natjoin([q,minny]).map{|t| t}
    }

  end
end

program = PriorityQ.new('localhost', ARGV[0])

(1..4).each do
  puts 'tick'
  program.tick
  program.out.each {|o| puts o.inspect}
  program.out2.each {|o| puts o.inspect}
end