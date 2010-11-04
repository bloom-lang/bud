require 'rubygems'
require 'bud'

module ChatProtocol
  def self.extended(base)
    base.channel :mcast, ['@to', 'from', 'username', 'time'], ['msg']
    base.channel :ctrl, ['@to', 'from', 'cmd']
    base.terminal :term
  end
end
