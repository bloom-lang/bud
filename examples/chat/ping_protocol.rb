module PingProtocol
  include BudModule

  state do
    channel :flow, [:@otherloc, :me, :msg, :wall, :budtick]
  end
end
