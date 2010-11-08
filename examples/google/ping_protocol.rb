module PingProtocol
  def state
    channel :flow, ['@otherloc', 'me', 'msg', 'wall', 'budtick']
  end
end
