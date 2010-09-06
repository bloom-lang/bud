require 'rubygems'
require 'bud'


class Sequencer < Bud
  def state
    serializer :ser, ['message']
    serializer :out, ['id', 'message']
    table :seq, ['id']
    scratch :input, ['message']
    scratch :buffer, ['message']
  end

  def declaration
    strata[0] = rules {
      seq << [0] if seq.empty?
      ser <= input.map{|i| i}

      buffer <= ser.map{|s| s}
      j = join [buffer, seq]
      seq <+ j.map{|b, s| [s.id + 1]}
      seq <- j.map{|b, s| [s.id]}
      out <= j.map{|b, s| [s.id, b.message]}
    }
  end
end


s = Sequencer.new("localhost", 1245)


s.tick

s.input <+ [['foo']]
s.input <+ [['bar']]
s.input <+ [['baz']]

s.tick
s.tick
s.tick
s.tick

print "ok\n"

s.out.each do |o| 
  print "OUT: #{o}\n"
end
s.tick
s.out.each do |o| 
  print "OUT: #{o}\n"
end
s.tick
s.out.each do |o| 
  print "OUT: #{o}\n"
end
