require 'test_common'

class Cascade
  include Bud

  state {
    table :t
    scratch :sc
    interface input, :sync
    interface input, :defer1
    interface input, :defer2
    interface input, :msg1
    channel :c
  }

  declare
  def program
    t <= sc <= sync
    t <= sc <+ defer1
    t <+ sc <= defer2
    t <= (c <~ msg1).payloads
  end
end

class TestCascade < Test::Unit::TestCase
  def test_casc
    p = Cascade.new({:port=>54321})
    assert_nothing_raised(RuntimeError) { p.run_bg }
    assert_nothing_raised(RuntimeError) { p.sync_do {
      p.sync <+ [['sync', 1]]
      p.defer1 <+ [['defer1', 1]]
      p.defer2 <+ [['defer2', 2]]
      p.msg1 <+ [['localhost:54321', ['msg1', 3]]]
    }}
    sleep 1
    assert_nothing_raised(RuntimeError) { p.stop_bg }    
    assert_equal([["defer1", 1], ["defer2", 2], ["msg1", 3], ["sync", 1]], p.t.to_a.sort)
  end
end