require 'rubygems'
require 'bud'
require 'bud/bud_module'
require 'anise'

module ChatProtocol 
  include Anise
  annotator :declare
  
  def state
    channel :mcast, ['@to', 'from', 'nick', 'time'], ['msg']
    channel :ctrl, ['@to', 'from', 'cmd']
  end

  #declare   
  #def foo
  #  ctrl <= term.map{|t| t}
  #end
end
