require 'rubygems'
require 'bud'

module ChatProtocol
  def state
    super if defined? super
    channel :mcast, ['@to', 'from', 'nick', 'time', 'msg']
    channel :ctrl, ['@to', 'from', 'cmd']
  end

  #declare   
  #def foo
  #  ctrl <= stdio.map{|t| t}
  #end
end
