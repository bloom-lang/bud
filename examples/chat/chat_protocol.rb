module ChatProtocol
  DEFAULT_ADDR = "localhost:12345"

  state do
    channel :mcast
    channel :connect
  end
end
