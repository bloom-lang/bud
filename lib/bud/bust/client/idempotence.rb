require 'rubygems'
require 'bud'

module Idempotence #:nodoc: all
  state do
    table :dead, [:dead]
  end

  def bust_idempotent(r) (dead.include? r) ? false : dead.insert(r) end
end
