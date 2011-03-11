module ChatProtocol
  include BudModule

  state do
    channel :mcast, [:@to, :from, :nick, :time, :msg]
    channel :signup
  end
end
