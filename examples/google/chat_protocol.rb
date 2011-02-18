module ChatProtocol
  include BudModule

  state do
    channel :mcast, [:@to, :from, :nick, :time, :msg]
    channel :ctrl, [:@to, :from, :cmd]
  end
end
