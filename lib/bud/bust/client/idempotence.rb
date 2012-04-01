require 'rubygems'
require 'bud'

module Idempotence #:nodoc: all
  state do
    table :dead, [:dead]
  end

  def bust_idempotent(r)
    if dead.include? r
      false
    else
      dead.insert(r)
      true
    end
  end
end
