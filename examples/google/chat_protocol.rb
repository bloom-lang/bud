module ChatProtocol
  def state
    super
    channel :mcast, [:@to, :from, :nick, :time, :msg]
    channel :ctrl, [:@to, :from, :cmd]
  end
end
