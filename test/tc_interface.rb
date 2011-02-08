require 'rubygems'
require 'bud'
require 'test/unit'

module MemberProtocol
  def state
    super
    interface input, :add_member, ['req_id'], ['name', 'addr']
    interface output, :result, ['req_id'], ['success']
    table :member, ['name'], ['addr']
  end
end

# Don't allow members whose names appear in "bad_people"
module SelectiveMembership
  include MemberProtocol
  include Anise
  annotator :declare

  def state
    super
    table :bad_people, ['name']
    scratch :good_add_reqs, ['req_id'], ['name', 'addr']
  end

  def bootstrap
    bad_people <= [['foo'], ['bar']]
  end

  declare
  def logic
    good_add_reqs <= add_member.map do |m|
      m unless bad_people.include? m.name
    end

    member <= good_add_reqs.map {|m| [m.name, m.addr]}
    result <= good_add_reqs.map {|m| [m.req_id, true]}
#    result <= add_member.map {|m| [m.req_id, false] unless good_add_reqs.include? m}
    result <= add_member.map {|m| [m.req_id, false] if bad_people.include? m.name}
    stdio <~ add_member.map {|m| ["got add_member!"]}
  end
end

class SimpleClient < Bud
  include SelectiveMembership
end

class InterfaceTest < Test::Unit::TestCase
  def test_basic
    c = SimpleClient.new
    c.run_bg
    puts "got to 1"
    c.sync_do {
      puts "got to 2"
      c.add_member <+ [[1, 'quux', c.ip_port]]
      puts "got to 3"
    }
    puts "got to 4"
    c.sync_do {}
    sleep 1
    puts "got to 5"
    c.stop_bg
    puts "got to 6"
  end
end
