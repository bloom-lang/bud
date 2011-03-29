require 'rubygems'
require 'bud'

class HelloWorld
  include Bud

  bloom do
    stdio <~ [["hello world!"]]
  end
end

HelloWorld.new.tick
