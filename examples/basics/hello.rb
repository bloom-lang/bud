require 'rubygems'
require 'bud'

class HelloWorld
  include Bud
  
  declare 
  def hi
    stdio <~ [["hello world!"]]
  end
end

HelloWorld.new.tick
  