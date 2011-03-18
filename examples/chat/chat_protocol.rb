module ChatProtocol
  state do
    channel :mcast, [:@to, :from, :nick, :time, :msg]
    channel :signup
  end
end
