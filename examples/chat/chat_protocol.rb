module ChatProtocol
  state do
    channel :mcast
    channel :connect
  end

  DEFAULT_ADDR = "localhost:12345"
end
