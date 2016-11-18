module ChatProtocol
  state do
    channel :connect, [:@addr, :client] => [:nick]
    channel :mcast
  end

  DEFAULT_ADDR = "127.0.0.1:12345"
end
