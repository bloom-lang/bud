require 'rubygems'
require 'bud'
require 'bud/bud_module'
require 'anise'

module ChatProtocol 
  include BudModule
  
  state do
    channel :mcast, [:@to, :from, :nick, :time] => [:msg]
    channel :ctrl, [:@to, :from, :cmd]
  end

  #declare   
  #def foo
  #  ctrl <= term.map{|t| t}
  #end
end
