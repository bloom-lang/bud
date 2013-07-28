require './test_common'
require 'bud/labeling/labeling'

module TestState
  state do
    interface input, :i1
    interface input, :i2
    channel :c1
    channel :c2
    table :guard1
    table :guard2
    interface output, :response
  end
end

module TestBasic
  include TestState
  bloom do
    c1 <~ i1
    c2 <~ i2
    guard1 <= c1
    guard2 <= c2
  end
end

module TestNM
  include TestBasic
  bloom do
    response <= guard1.notin(guard2, :val => :val)
  end
end

module TestGroup
  include TestBasic
  bloom do
    response <= guard1.group([:val], count)
  end
end

module TestMono
  include TestBasic
  bloom do
    response <= (guard1 * guard2).lefts(:val => :val)
  end
end

module TestDeletion
  include TestMono
  state do
    interface input, :dguard
    channel :c3
  end
  bloom do
    c3 <~ dguard
    guard2 <- (guard2 * c3).lefts(:val => :val)
  end
end

module TestNestMod
  import TestNM => :tnm
  state do
    interface input, :inn1
    interface input, :inn2
    interface output, :outt
  end

  bloom do
    tnm.i1 <= inn1
    tnm.i2 <= inn2
    outt <= tnm.response
  end
end

# ``unguarded asynchrony'' is a hidden source of nondeterminism in otherwise
# monotonic bloom programs.
module JoinProto
  state do
    interface input, :ileft
    interface input, :iright
    interface output, :result
  end
end

module Buffers
  include JoinProto
  state do
    scratch :ls
    scratch :rs
    table :lt
    table :rt

    channel :lchan
    channel :rchan
  end

  bloom do
    ileft <= lchan
    iright <= rchan

    ls <= ileft
    rs <= iright
    lt <= ileft
    rt <= iright
  end
end

module BugButt
  include Buffers
  bloom do
    result <= (ls * rs).lefts
  end
end

module HalfGuard
  include Buffers
  bloom do
    result <= (lt * rs).lefts
  end
end

module FullGuard
  include Buffers
  bloom do
    result <= (lt * rt).lefts
  end
end



module BB
  include Validate
  include GuardedAsync
  include Bud
end

class RolledUp
  include BB
  include TestNM
end

class RollupMono
  include BB
  include TestMono
end

class RollGroup
  include BB
  include TestGroup
end

class RollDels
  include BB
  include TestDeletion
end

class RollNest
  include BB
  include TestNestMod
end

module BBG
  include BB
  state do
    interface input, :ul
    interface input, :ur
  end
  bloom do
    lchan <~ ul
    rchan <~ ur
  end
end

class RollHG
  include BBG
  include HalfGuard
end

class TestBlazes < MiniTest::Unit::TestCase
  def test_label1
    r = RolledUp.new
    r.tick
    report = r.validate
    assert(report.map{|r| r.to_a.last}.include?(["D"]), "flow not reported as divergent : #{report}")
  end

  def test_label2
    r = RollGroup.new
    r.tick
    report = r.validate
    assert(report.map{|r| r.to_a.last}.include?(["D"]), "flow not reported as divergent")
  end

  def test_mono
    r = RollupMono.new
    r.tick
    report = r.validate
    assert(!report.map{|r| r.to_a.last}.include?(["D"]), "flow not reported as confluent: #{report}")
  end

  def test_deletion
    r = RollDels.new
    r.tick
    report = r.validate
    reps = report.map{|r| [r[0], r[1], r.last]}
    assert(reps.include?(["dguard", "response", ["D"]]), "deletion path not marked D")
    assert(reps.include?(["i2", "response", ["D"]]), "main path not marked D #{reps}")
  end

  def test_nesting
    r = RollNest.new
    r.tick
    report = r.validate
    assert(report.map{|r| r.to_a.last}.include?(["D"]), "flow not reported as divergent : #{report}")
  end

  def test_unguarded
    h = RollHG.new
    h.tick
    report = h.validate
    assert(report.map{|r| r.to_a.last}.include?(["D"]), "flow not reported as divergent : #{report}")
  end

  def test_labeler1
    l = Label.new("TestNM")
    assert_equal({"response" => "D"}, l.output_report)
    assert_equal({"response" => {"i1" => "A", "i2" => "D"}}, l.path_report)
  end

  def test_labeler2
    l = Label.new("TestGroup")
    assert_equal({"response" => "D"}, l.output_report)
    assert_equal({"response" => {"i1" => "D"}}, l.path_report)
  end

  def test_labeler3
    l = Label.new("TestMono")
    assert_equal({"response" => "A"}, l.output_report)
    assert_equal({"response" => {"i1" => "A", "i2" => "A"}}, l.path_report)
  end
end


# Tests covering just the GA part
module Extry
  include Bud
  include GuardedAsync

  state do
    table :rem_race, channel_race.schema
  end
  bloom do
    rem_race <= channel_race
  end
end

class BugC
  include Extry
  include BugButt
end

class HalfGuardC
  include Extry
  include HalfGuard
end

class FullGuardC
  include Extry
  include FullGuard
end

class TestBlazes < MiniTest::Unit::TestCase
  def test_bug
    c = BugC.new
    c.tick
    assert_equal([["rchan", "lchan", "result", false],
                  ["lchan", "rchan", "result", false]].to_set,
                 c.rem_race.map{|r| r.to_a}.to_set)
  end

  def test_hg
    c = HalfGuardC.new
    c.tick
    assert_equal([["rchan", "lchan", "result", false],
                  ["lchan", "rchan", "result", false]].to_set,
                 c.rem_race.map{|r| r.to_a}.to_set)
  end

  def test_full
    c = FullGuardC.new
    c.tick
    assert_equal([["rchan", "lchan", "result", true],
                  ["lchan", "rchan", "result", true]].to_set,
                 c.rem_race.map{|r| r.to_a}.to_set)
  end
end
