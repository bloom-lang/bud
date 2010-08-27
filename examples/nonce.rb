require 'rubygems'
require 'bud'


class Nonce < Bud
  def state
    table :nonce, ['id']
  end

  def declaration
    strata[0] = rules {
      if nonce.empty?
        r = rand(100000)
        nonce << [[r]]
      end
    }
  end
end

#n = Nonce.new('localhost', 12345)
#print "n is #{n.class}\n"

#(0..10).each do |i|
#  n.tick
#  n.nonce.each do |nonce|
#    print "N: #{nonce}\n"
#  end 
#  n.nonce <- n.nonce.map{|nonce| nonce}
#end



