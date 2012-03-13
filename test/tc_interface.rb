require './test_common'

module MemberProtocol
  state do
    interface input, :add_member, [:req_id] => [:name, :addr]
    interface output, :result, [:req_id] => [:success]
    table :member, [:name] => [:addr]
  end
end

# Don't allow members whose names appear in "bad_people"
module SelectiveMembership
  include MemberProtocol

  state do
    table :bad_people, [:name]
    scratch :good_add_reqs, [:req_id] => [:name, :addr]
  end

  bootstrap do
    bad_people <= [['foo'], ['bar']]
  end

  bloom do
    good_add_reqs <= add_member.map do |m|
      m unless bad_people.include? [m.name]
    end

    member <= good_add_reqs.map {|m| [m.name, m.addr]}
    result <= good_add_reqs.map {|m| [m.req_id, true]}
    result <= add_member.map {|m| [m.req_id, false] unless good_add_reqs.include? m}
  end
end

class SimpleClient
  include Bud
  include SelectiveMembership
end

class InterfaceTest < MiniTest::Unit::TestCase
  def test_basic
    c = SimpleClient.new
    c.run_bg

    # Add a legal member
    c.sync_do {
      c.add_member <+ [[1, 'quux', c.ip_port]]
    }
    c.sync_do {
      assert_equal(1, c.result.length)
      assert_equal([1, true], c.result.first)
    }
    # Test that output interface flushed after tick
    c.sync_do {
      assert(c.result.empty?)
    }

    # Add two members, one is illegal
    c.sync_do {
      c.add_member <+ [[2, 'foo', c.ip_port], [3, 'baz', c.ip_port]]
    }
    c.sync_do {
      results = c.result.to_a.sort
      assert_equal([[2, false], [3, true]], results)
    }
    c.stop
  end
end
