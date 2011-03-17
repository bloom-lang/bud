require 'rubygems'
require 'bud'

module Idempotence
  include BudModule

  state do
    table :dead, [:dead]
  end

  def idempotent(r) (dead.include? r) ? false : dead.insert(r) end
end
