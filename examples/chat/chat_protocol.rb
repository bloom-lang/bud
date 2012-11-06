module ChatProtocol
  state do
    channel :connect, [:@addr, :client] => [:nick]
    channel :mcast
  end

  DEFAULT_ADDR = "localhost:12345"
end
