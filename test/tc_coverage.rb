# gratuitous tests to ensure code coverage
require 'test_common'

class Nada
  include Bud

  state do
    table :num, [:num]
  end

  bloom do
    num <= (1..5).map{|i| [i]}
  end
end

# lib/bud.rb
class GratuitousBud < Test::Unit::TestCase
  def kill_child_with_sig(signal)
    pid = fork do
      p = Nada.new
      p.run_fg
    end
    sleep 1
    Process.kill(signal, pid)
    Process.waitpid(pid)
  end

  def test_sigint
    kill_child_with_sig("INT")
  end

  def test_sigterm
    kill_child_with_sig("TERM")
  end
end
