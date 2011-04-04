require 'rubygems'
require 'bud'

module Idempotence # :nodoc: all
  state do
    table :dead, [:dead]
  end

  def idempotent(r) (dead.include? r) ? false : dead.insert(r) end
end
