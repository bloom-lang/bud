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
      minny <= q.group(nil, min(q.priority))
      q <- out.map{|t| t}
    }
    
    strata[2] = rules {
      out2 <= natjoin([q,minny]).map{|q, m| q+m}
    }

  end
end

program = PriorityQ.new('localhost', ARGV[0])

(1..4).each do
  puts '---tick---'
  program.tick
  program.out.each {|o| puts "argmin: #{o.inspect}"}
  program.out2.each {|o| puts "joinmin: #{o.inspect}"}
end