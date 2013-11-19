require './test_common'

class RseSimple
  include Bud

  state do
    table :sbuf, [:id] => [:val]
    scratch :res, sbuf.schema
    table :res_approx, sbuf.schema
  end

  bloom do
    res <= sbuf.notin(res_approx)
  end
end

class RseQual
  include Bud

  state do
    table :sbuf, [:id] => [:val]
    scratch :res, sbuf.schema
    table :sbuf_val_seen, [:val]
  end

  bloom do
    res <= sbuf.notin(sbuf_val_seen, :val => :val)
  end
end

class RseChainedNeg
  include Bud

  state do
    table :t0
    table :t1
    table :t2
    table :t3
    table :t4
    table :t5
    table :t6
  end

  bloom do
    # We can reclaim a tuple if it appears in t6 AND (t3 OR t4 OR r5)
    t0 <= t2.notin(t3).notin(t4).notin(t5)
    t1 <= t2.notin(t6)
  end
end

class RseNegateIntersect
  include Bud

  state do
    table :res1
    table :res2
    table :res3
    table :res4
    table :t2
    table :t3
    table :t4
    table :t5
    table :t6
  end

  bloom do
    # We can reclaim t2 tuples when they appear in _all of_ t3, t4, t5, and t6.
    res1 <= t2.notin(t3)
    res2 <= t2.notin(t4)
    res3 <= t2.notin(t5)
    res4 <= t2.notin(t6)
  end
end

# Check that we don't try to apply RSE on relation t if t appears in 1+ legal
# contexts and at least one illegal context.
class RseNegateIntersectDelete
  include Bud

  state do
    table :res1
    table :res2
    table :res3
    table :t2
    table :t3
    table :t4
    table :t5
    scratch :some_event
  end

  bloom do
    res1 <= t2.notin(t3)        # Okay
    res2 <= t2.notin(t4)        # Okay
    res3 <= t2.notin(t5)        # Not okay because t5 is deleted from
    t5 <- some_event
  end
end

# We can apply RSE to a rule even if the LHS collection of the rule is deleted
# from (or isn't persistent in the first place).
class RseDeleteDownstream
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :some_event
  end

  bloom do
    t1 <= t2.notin(t3)
    t1 <- some_event
  end
end

class RseNegateScratchLhs
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :r1
    scratch :r2
  end

  bloom do
    # Despite the fact that r2 is not persistent, we can reclaim t1 tuples once
    # they appear in both t2 and t3 -- because r2 is derived from t1 via
    # negating t1 against t3.
    r1 <= t1.notin(t2)
    r2 <= t1.notin(t3)
  end
end

class RseNegateScratchLhsBad
  include Bud

  state do
    table :t1
    table :t2
    scratch :r1
    scratch :r2
    scratch :r3
  end

  bloom do
    r1 <= t1.notin(t2)
    r2 <= t1.notin(r3)
  end
end

class RseNegateScratchLhsBad2
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :r1
    scratch :r2
    scratch :r3
  end

  bloom do
    r1 <= t1.notin(t2)
    r2 <= t1.notin(t3).notin(r3)
  end
end

class RseNegateScratchLhsBad3
  include Bud

  state do
    table :t1
    table :t2
    scratch :r1
    scratch :r2
  end

  bloom do
    # We can't reclaim from t1, because that would cause r2 to shrink -- and we
    # regard r2 as potentially an "output" collection.
    r1 <= t1.notin(t2)
    r2 <= t1
  end
end

class RseNegateScratchRhs
  include Bud

  state do
    scratch :r1
    table :t1
    scratch :t2
    table :t3

    scratch :r2
    table :t4
    scratch :t5
    table :t6
    table :t7

    scratch :r3
    table :t8
    scratch :t9
    scratch :t10
    scratch :t11
    table :t12
    table :t20
  end

  bloom do
    # We can reclaim from baz: even though it is a scratch, it is derived purely
    # from persistent tables via monotone rules.
    r1 <= t1.notin(t2)
    t2 <= t3 {|q| [q.val, q.key]}

    r2 <= t4.notin(t5)
    t5 <= (t6 * t7).pairs(:val => :key) {|x,y| [x.key, y.val]}
    t5 <= (t5 * t20).lefts

    r3 <= t8.notin(t9)
    t9 <= t10 {|t| [t.key + 1, t.val]}
    t10 <= t11 {|t| [t.key, t.val + 1]}
    t11 <= t12
  end
end

class RseNegateScratchRhsBad
  include Bud

  state do
    scratch :r1
    table :t1
    scratch :t2
    scratch :t3
    table :t4

    scratch :r2
    table :t5
    scratch :t6
    table :t7
    scratch :t8

    scratch :r3, [:x, :y]
    table :t9, [:x, :y]
    scratch :t10, [:x, :y]
    channel :t11, [:@x, :y]
    table :t12, [:x, :y]
  end

  bloom do
    # Scratch t2 derived via non-monotone rule
    r1 <= t1.notin(t2)
    t2 <= t3.notin(t4)

    # Scratch t6 derived from a scratch that isn't grounded in a persistent
    # table
    r2 <= t5.notin(t6)
    t6 <= t7
    t6 <= t8

    # Scratch t10 derived from a channel; the channel has a sender-side
    # persistent ground, but that doesn't matter
    r3 <= t9.notin(t10)
    t10 <= t11
    t11 <~ t12
  end
end

# Situations where a reference to the reclaimed relation on the RHS of a rule
# SHOULD NOT prohibit RSE.
class RseRhsRef
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
    table :t5
    table :t6
    table :t7
    scratch :s1
    scratch :s2
    scratch :res
  end

  bloom do
    # Via RSE (for a different table), we infer a deletion rule for the
    # downstream persistent table -- but since the rule is created by RSE, we
    # know it is "safe" and can be ignored.
    t6 <= t1
    s2 <= t6.notin(t7)
    res <= t1.notin(t2)

    # Other rules can have t1 on their RHS, provided they (a) are monotone (b)
    # derive into a persistent table.
    t3 <= t1                                                    # identity
    t4 <= t1 {|t| [t.key + 100, t.val + 100] if t.key < 100}    # sel, proj

    # t1 appears on the RHS of a rule that derives into a scratch, but the
    # output of the scratch is later persisted.
    s1 <= t1
    t5 <= s1
  end
end

# Situations where a reference to the reclaimed relation on the RHS of a rule
# SHOULD prohibit RSE.
class RseRhsRefBad
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
    table :t5
    table :t6, [:cnt]
    table :t7
    table :t8
    table :t9
    table :t10
    table :t11
    table :t12
    table :t13
    table :t14
    table :t15
    table :t16
    scratch :out
    scratch :some_event
    scratch :res
  end

  bloom do
    # Deletion from a persistent table
    res <= t1.notin(t2)
    t3 <= t1
    t3 <- some_event

    # Reference in a grouping/agg expression
    res <= t4.notin(t5)
    t6 <= t4.group(nil, count)

    # Reference as the outer (negative / NM) operand to a notin
    res <= t7.notin(t8)
    t9 <= t10.notin(t7)

    # Dataflow reaches both a persistent and a transient-output collection. We
    # don't want to delete from t7 in this circumstance, because we regard the
    # content of "output" as needing to be preserved.
    res <= t11.notin(t12)
    t9 <= t11
    out <= t11

    # Reference as the outer (negative / NM) operand to a notin with a code
    # block
    res <= t13.notin(t14)
    t9 <= t15.notin(t13) {|x,y| x != y}
  end
end

class JoinRse
  include Bud

  state do
    table :node, [:addr, :epoch]
    table :sbuf, [:id] => [:epoch, :val]
    scratch :res, [:addr] + sbuf.cols
    table :res_approx, res.schema
  end

  bloom do
    res <= (sbuf * node).pairs(:epoch => :epoch) {|s,n| [n.addr] + s}.notin(res_approx)
  end
end

class JoinRseVariantQuals
  include Bud

  state do
    table :node, [:addr, :epoch]
    table :sbuf, [:id] => [:epoch, :val]
    scratch :res, [:addr] + sbuf.cols
    table :res_approx, res.schema
  end

  bloom do
    res <= (sbuf * node).pairs(node.epoch => sbuf.epoch) {|s,n| [n.addr] + s}.notin(res_approx)
  end
end

# RSE for joins with no join predicate -- i.e., cartesian products
class JoinRseNoQual
  include Bud

  state do
    table :node, [:addr]
    table :sbuf, [:id] => [:val]
    scratch :res, sbuf.cols + node.cols # Reverse column order for fun
    table :res_approx, res.schema
  end

  bloom do
    res <= ((sbuf * node).pairs {|s,n| s + n}).notin(res_approx)
  end
end

class JoinRseSealed
  include Bud

  state do
    sealed :node, [:addr]
    table :sbuf, [:id] => [:val]
    scratch :res, sbuf.cols + node.cols # Reverse column order for fun
    table :res_approx, res.schema
  end

  bootstrap do
    node <= [["foo"], ["bar"]]
  end

  bloom do
    res <= ((sbuf * node).pairs {|s,n| s + n}).notin(res_approx)
  end
end

class JoinRseNegationQual
  include Bud

  state do
    sealed :node, [:addr]
    table :sbuf, [:id] => [:val]
    scratch :res, [:addr, :id] => [:val]
    table :res_approx, res.key_cols + [:garbage]
  end

  bootstrap do
    node <= [["foo"], ["bar"]]
  end

  bloom do
    res <= ((node * sbuf).pairs {|n,s| n + s}).notin(res_approx, 0 => :addr, 1 => 1)
  end
end

class JoinRseNegationQualVariant
  include Bud

  state do
    sealed :node, [:addr]
    table :sbuf, [:id] => [:val]
    scratch :res, [:addr, :id] => [:val]
    table :res_approx, [:garbage] + res.key_cols
  end

  bootstrap do
    node <= [["foo"], ["bar"]]
  end

  bloom do
    res <= ((node * sbuf).pairs {|n,s| n + s}).notin(res_approx, 1 => :id)
  end
end

class JoinRseSealedUseTwice
  include Bud

  state do
    sealed :node, [:addr]
    channel :ins_chn, [:@addr, :id]
    channel :del_chn, [:@addr, :id]
    table :ins_log, [:id]
    table :del_log, [:id]
  end

  bloom do
    ins_chn <~ (node * ins_log).pairs {|n,l| n + l}
    del_chn <~ (node * del_log).pairs {|n,l| n + l}

    ins_log <= ins_chn.payloads
    del_log <= del_chn.payloads
  end
end

class JoinRseUseTwice
  include Bud

  state do
    sealed :node, [:addr, :epoch]
    channel :ins_chn, [:@addr, :id] => [:epoch]
    channel :del_chn, [:@addr, :id] => [:epoch]
    table :ins_log, [:id] => [:epoch]
    table :del_log, [:id] => [:epoch]
  end

  bloom do
    ins_chn <~ (node * ins_log).pairs(:epoch => :epoch) {|n,l| n + l}
    del_chn <~ (node * del_log).pairs(:epoch => :epoch) {|n,l| n + l}

    ins_log <= ins_chn.payloads
    del_log <= del_chn.payloads
  end
end

# Given sealed collection n that appears in two RSE-eligible join rules:
#
#   (n * # r).pairs.notin(...)
#   (n * s).pairs.notin(...)
#
# We only want to reclaim from n when we see seals for _both_ r and s.
# Naturally, the two joins might have different quals (and hence different
# sealing conditions).
class JoinRseSealDoubleReclaim
  include Bud

  state do
    sealed :node, [:addr, :epoch_x, :epoch_y]
    table :x_log, [:id, :epoch]
    table :y_log, [:id, :epoch]
    table :x_res, [:addr, :epoch_x, :epoch_y, :id, :epoch]
    table :y_res, x_res.schema
    table :x_res_approx, x_res.schema
    table :y_res_approx, x_res.schema
  end

  bloom do
    x_res <= ((node * x_log).pairs(:epoch_x => :epoch) {|n,x| n + x}).notin(x_res_approx)
    y_res <= ((node * y_log).pairs(:epoch_y => :epoch) {|n,y| n + y}).notin(y_res_approx)
  end
end

class JoinRseDoubleScratch
  include Bud

  state do
    table :obj, [:oid] => [:val]
    table :ref, [:id] => [:name, :obj_id]
    table :del_ref, [:id] => [:del_id]

    scratch :view, ref.cols + obj.cols
    scratch :view2, view.schema
  end

  bloom do
    view  <= ((ref * obj).pairs(:obj_id => :oid) {|r,o| r + o}).notin(del_ref, 0 => :del_id)
    view2 <= ((ref * obj).pairs(:obj_id => :oid) {|r,o| r + o}).notin(del_ref, 0 => :del_id)
  end
end

class JoinRseTlistConst
  include Bud

  state do
    table :a, [:c1, :c2, :c3]
    table :a_approx, a.schema
    table :b
    sealed :c
  end

  bloom do
    a <= ((b * c).pairs {|t1,t2| [t1.key, "foo", t2.val]}).notin(a_approx)
    a <= ((b * c).pairs {|t1,t2| [t1.key, 99, t2.val]}).notin(a_approx)
  end
end

class JoinRseTlistIpPort
  include Bud

  state do
    table :a, [:c1, :c2, :c3, :c4]
    table :a_approx, a.schema
    table :b
    sealed :c
  end

  bloom do
    a <= ((b * c).pairs {|t1,t2| [t1.key, ip_port, port, t2.val]}).notin(a_approx)
  end
end

class JoinRseTlistConstQual
  include Bud

  state do
    table :a, [:c1, :c2, :c3]
    table :a_approx, a.schema
    table :b
    sealed :c
  end

  bloom do
    a <= ((b * c).pairs {|t1,t2| [t1.key, port, t2.val]}).notin(a_approx, 0 => :c1, 1 => :c2)
  end
end

class JoinRseOuter
  include Bud

  state do
    table :t1, [:a, :b]
    table :t2, [:c, :d]
    table :t3, [:e, :f]
    scratch :r1
  end

  bloom do
    r1 <= (t1 * t2).outer(:a => :c) {|x,y| [x.b, y.d]}.notin(t3)
  end
end

class JoinRseNotinPullup
  include Bud

  state do
    sealed :ref, [:id] => [:name, :obj_id]
    table :del_ref, [:id] => [:del_id]
    table :obj, [:id] => [:val]

    scratch :view, [:name, :ref_id, :val]
  end

  bloom do
    view <= ((ref * obj).pairs(:obj_id => :id) {|r,o| [r.name, r.id, o.val]}).notin(del_ref, 1 => :del_id)
  end
end

class JoinRseNotinPullupImplicitQual
  include Bud

  state do
    sealed :ref, [:id] => [:name, :obj_id]
    table :obj, [:id] => [:val]
    table :del_ref, [:del_id]

    scratch :live_ref, [:ref_id]
  end

  bloom do
    live_ref <= ((ref * obj).pairs(:obj_id => :id) {|r,o| [r.id]}).notin(del_ref)
  end
end

class JoinRseNotinPullupImplicitQualLefts
  include Bud

  state do
    sealed :ref, [:id] => [:name, :obj_id]
    table :obj, [:id] => [:val]
    table :del_ref, [:del_id]

    scratch :live_ref, [:ref_id]
  end

  bloom do
    live_ref <= ((ref * obj).lefts(:obj_id => :id) {|r| [r.id]}).notin(del_ref)
  end
end

class RseJoinRecursion
  include Bud

  state do
    table :safe_log, [:id] => [:key, :val, :deps]
    scratch :dep, [:from, :to]
    scratch :dep_tc, [:from, :to]
    scratch :view, safe_log.schema
  end

  bloom do
    dep <= safe_log.flat_map {|l| l.deps.map {|d| [d, l.id]}}
    dep_tc <= (dep * dep_tc).pairs(:to => :from) {|d,t| [d.from, t.to]}
    view <= safe_log.notin(dep_tc, :id => :from)
  end
end

class RseJoinRhs
  include Bud

  state do
    table :safe_log, [:id] => [:key, :val]
    table :dominated, [:id]
    scratch :view, safe_log.schema
  end

  bloom do
    dominated <= (safe_log * safe_log).pairs(:key => :val) do |w1,w2|
      [w1.id] if w1 != w2
    end
    view <= safe_log.notin(dominated, :id => :id)
  end
end

class PeterBug
  include Bud

  state do
    table :in1, [:id]
    table :in2, [:id]
    table :in3, [:id]

    scratch :out1, in1.schema
    scratch :out2, in1.schema
    scratch :out3, in1.schema
  end

  bloom do
    out1 <= in1.notin(in2, :id => :id)
    out2 <= in1
    out3 <= out2.notin(in3, :id => :id)
  end
end

class ReclaimLeftsNoBlock
  include Bud

  state do
    table :write, [:wid] => [:batch]
    table :commit, [:batch]
    table :write_log, [:wid]
    scratch :commit_event, write.schema
  end

  bloom do
    commit_event <= (write * commit).lefts(:batch => :batch).notin(write_log, 0 => :wid)
  end
end

class RseFromRange
  include Bud

  state do
    sealed :node, [:addr]
    channel :log_commit_chn, [:@addr, :id]
    range :log_commit, [:id]
  end

  bloom do
    log_commit_chn <~ (node * log_commit).pairs {|n,c| n + c}
    log_commit <= log_commit_chn.payloads
  end
end

class TestRse < MiniTest::Unit::TestCase
  def test_rse_simple
    s = RseSimple.new
    s.sbuf <+ [[5, 10], [6, 12]]
    s.tick
    s.res_approx <+ [[5, 10]]
    s.tick
    s.tick

    assert_equal([[6, 12]], s.sbuf.to_a.sort)
  end

  def test_rse_qual
    s = RseQual.new
    s.sbuf <+ [[1, 5], [2, 5], [3, 6]]
    s.tick
    assert_equal([[1, 5], [2, 5], [3, 6]].sort, s.res.to_a.sort)

    s.sbuf_val_seen <+ [[5]]
    s.tick
    s.tick

    assert_equal([[3, 6]], s.res.to_a.sort)
    assert_equal([[3, 6]], s.sbuf.to_a.sort)
  end

  def test_rse_chained_neg
    s = RseChainedNeg.new
    s.t2 <+ [[1, 1], [2, 2], [3, 3]]
    s.t3 <+ [[2, 2]]
    s.t4 <+ [[3, 3]]
    s.t6 <+ [[2, 2], [3, 3]]
    2.times { s.tick }

    assert_equal([[1, 1]], s.t2.to_a.sort)

    s.t2 <+ [[4, 4], [5, 5]]
    s.t5 <+ [[4, 4]]
    2.times { s.tick }

    assert_equal([[1, 1], [4, 4], [5, 5]], s.t2.to_a.sort)

    s.t2 <+ [[6, 6]]
    s.t3 <+ [[5, 5], [6, 6]]
    s.t6 <+ [[4, 4], [5, 5]]
    2.times { s.tick }

    assert_equal([[1, 1], [6, 6]], s.t2.to_a.sort)
  end

  def test_rse_negate_intersect
    s = RseNegateIntersect.new
    s.t2 <+ [[5, 10], [6, 11], [7, 12]]
    s.t3 <+ [[5, 10], [7, 12]]
    s.t4 <+ [[6, 11]]
    s.t5 <+ [[7, 12], [6, 11]]
    s.t6 <+ [[7, 12], [5, 10]]
    2.times { s.tick }

    assert_equal([[5, 10], [6, 11], [7, 12]], s.t2.to_a.sort)

    s.t2 <+ [[8, 13], [9, 14]]
    s.t3 <+ [[6, 11], [8, 13]]
    s.t4 <+ [[5, 10], [8, 13]]
    s.t5 <+ [[5, 10], [8, 13]]
    s.t6 <+ [[6, 11], [8, 13]]
    2.times { s.tick }

    assert_equal([[7, 12], [9, 14]], s.t2.to_a.sort)
  end

  def test_rse_negate_intersect_del
    s = RseNegateIntersectDelete.new
    s.t2 <+ [[5, 10], [6, 11], [7, 12]]
    s.t3 <+ [[5, 10]]
    s.t4 <+ [[6, 11]]
    s.t5 <+ [[7, 12]]
    2.times { s.tick }

    assert_equal([[5, 10], [6, 11], [7, 12]], s.t2.to_a.sort)

    s.t3 <+ [[6, 11], [7, 12]]
    s.t4 <+ [[5, 10], [7, 12]]
    s.t4 <+ [[5, 10], [6, 11]]
    2.times { s.tick }

    assert_equal([[5, 10], [6, 11], [7, 12]], s.t2.to_a.sort)
  end

  def test_rse_delete_downstream
    s = RseDeleteDownstream.new
    s.t2 <+ [[5, 10], [6, 11]]
    s.t3 <+ [[5, 10]]
    2.times { s.tick }

    assert_equal([[6, 11]], s.t2.to_a.sort)
  end

  def test_rse_negate_scratch_lhs
    s = RseNegateScratchLhs.new
    s.t1 <+ [[5, 10], [6, 11]]
    s.t2 <+ [[6, 11]]
    s.t3 <+ [[6, 11]]
    2.times { s.tick }

    assert_equal([[5, 10]], s.t1.to_a.sort)
  end

  def test_rse_negate_scratch_lhs_bad
    s = RseNegateScratchLhsBad.new
    s.t1 <+ [[5, 10], [6, 11]]
    s.t2 <+ [[6, 11]]
    2.times { s.tick }

    assert_equal([[5, 10], [6, 11]], s.t1.to_a.sort)
  end

  def test_rse_negate_scratch_lhs_bad2
    s = RseNegateScratchLhsBad2.new
    s.t1 <+ [[5, 10], [6, 11]]
    s.t2 <+ [[6, 11]]
    s.t3 <+ [[6, 11]]
    2.times do
      s.r3 <+ [[6, 11]]
      s.tick
    end

    assert_equal([[5, 10], [6, 11]], s.t1.to_a.sort)
  end

  def test_rse_negate_scratch_lhs_bad3
    s = RseNegateScratchLhsBad3.new
    s.t1 <+ [[5, 10], [6, 11]]
    s.t2 <+ [[6, 11]]
    2.times { s.tick }

    assert_equal([[5, 10], [6, 11]], s.t1.to_a.sort)
  end

  def test_rse_negate_scratch_rhs
    j = RseNegateScratchRhs.new
    j.t1 <+ [[5,10], [6, 11]]
    j.t3 <+ [[11, 6], [12, 7]]
    j.t4 <+ [[5, 10], [6, 11]]
    j.t6 <+ [[5, 99]]
    j.t7 <+ [[99, 10]]
    j.t8 <+ [[5, 10], [6, 11]]
    j.t12 <+ [[5, 10], [6, 11]]
    2.times { j.tick }

    assert_equal([[5, 10]].to_set, j.r1.to_set)
    assert_equal([[5, 10]].to_set, j.t1.to_set)
    assert_equal([[6, 11]].to_set, j.r2.to_set)
    assert_equal([[6, 11]].to_set, j.t4.to_set)
    assert_equal([[5, 10]].to_set, j.r3.to_set)
    assert_equal([[5, 10]].to_set, j.t8.to_set)
  end

  def test_rse_negate_scratch_rhs_bad
    j = RseNegateScratchRhsBad.new(:port => 5558)
    j.t1 <+ [[5, 10], [6, 11]]
    j.t3 <+ [[6, 11], [7, 12]]
    j.t5 <+ [[5, 10], [6, 11]]
    j.t7 <+ [[6, 11], [7, 12]]
    j.t9 <+ [["localhost:#{j.port}", 100],
             ["localhost:#{j.port}", 101]]
    j.t12 <+ [["localhost:#{j.port}", 100]]
    7.times { j.tick; sleep 0.1 }

    assert_equal([[5, 10], [6, 11]].to_set, j.t1.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, j.t5.to_set)
    assert_equal([["localhost:#{j.port}", 100],
                  ["localhost:#{j.port}", 101]].to_set, j.t9.to_set)
  end

  def test_rse_rhs_ref
    s = RseRhsRef.new
    s.t1 <+ [[1, 1], [2, 2]]
    s.t2 <+ [[2, 2], [3, 3]]
    2.times { s.tick }

    assert_equal([[1, 1]], s.t1.to_a.sort)
  end

  def test_rse_rhs_ref_bad
    s = RseRhsRefBad.new
    s.t1 <+ [[1, 1], [2, 2]]
    s.t2 <+ [[2, 2], [3, 3]]
    s.t4 <+ [[1, 1], [2, 2]]
    s.t5 <+ [[2, 2], [3, 3]]
    s.t7 <+ [[1, 1], [2, 2]]
    s.t8 <+ [[2, 2], [3, 3]]
    s.t11 <+ [[1, 1], [2, 2]]
    s.t12 <+ [[2, 2], [3, 3]]
    s.t13 <+ [[1, 1], [2, 2]]
    s.t14 <+ [[2, 2], [3, 3]]
    2.times { s.tick }

    assert_equal([[1, 1], [2, 2]], s.t1.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t4.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t7.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t11.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t13.to_a.sort)
  end

  def test_join_rse
    j = JoinRse.new
    j.node <+ [["foo", 1], ["bar", 1], ["bar", 2]]
    j.sbuf <+ [[100, 1, "x"], [101, 1, "y"]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2], ["foo", 1]], j.node.to_a.sort)

    j.res_approx <+ [["foo", 100, 1, "x"], ["foo", 101, 1, "y"]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2], ["foo", 1]], j.node.to_a.sort)

    # No more messages in epoch 1
    j.seal_sbuf_epoch <+ [[1]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    # No more node addresses in epoch 1
    j.seal_node_epoch <+ [[1]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    j.res_approx <+ [["bar", 100, 1, "x"]]
    2.times { j.tick }
    assert_equal([[101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    j.res_approx <+ [["bar", 101, 1, "y"]]
    2.times { j.tick }
    assert_equal([], j.sbuf.to_a.sort)
    assert_equal([["bar", 2]], j.node.to_a.sort)
  end

  def test_join_rse_variant_qual
    j = JoinRseVariantQuals.new
    j.node <+ [["foo", 1], ["bar", 1], ["bar", 2]]
    j.sbuf <+ [[100, 1, "x"], [101, 1, "y"]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2], ["foo", 1]], j.node.to_a.sort)

    j.res_approx <+ [["foo", 100, 1, "x"], ["foo", 101, 1, "y"]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2], ["foo", 1]], j.node.to_a.sort)

    # No more messages in epoch 1
    j.seal_sbuf_epoch <+ [[1]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    # No more node addresses in epoch 1
    j.seal_node_epoch <+ [[1]]
    2.times { j.tick }
    assert_equal([[100, 1, "x"], [101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    j.res_approx <+ [["bar", 100, 1, "x"]]
    2.times { j.tick }
    assert_equal([[101, 1, "y"]], j.sbuf.to_a.sort)
    assert_equal([["bar", 1], ["bar", 2]], j.node.to_a.sort)

    j.res_approx <+ [["bar", 101, 1, "y"]]
    2.times { j.tick }
    assert_equal([], j.sbuf.to_a.sort)
    assert_equal([["bar", 2]], j.node.to_a.sort)
  end

  def test_join_rse_no_qual
    j = JoinRseNoQual.new
    j.node <+ [["foo"], ["bar"]]
    j.sbuf <+ [[1, "x"], [2, "y"], [3, "z"]]
    2.times { j. tick }
    assert_equal([["bar"], ["foo"]], j.node.to_a.sort)
    assert_equal([[1, "x"], [2, "y"], [3, "z"]], j.sbuf.to_a.sort)

    j.seal_node <+ [["..."]]
    2.times { j. tick }
    assert_equal([["bar"], ["foo"]], j.node.to_a.sort)
    assert_equal([[1, "x"], [2, "y"], [3, "z"]], j.sbuf.to_a.sort)

    j.res_approx <+ [[1, "x", "foo"], [2, "y", "bar"],
                     [3, "z", "foo"], [3, "z", "bar"]]
    2.times { j. tick }
    assert_equal([["bar"], ["foo"]], j.node.to_a.sort)
    assert_equal([[1, "x"], [2, "y"]], j.sbuf.to_a.sort)

    j.res_approx <+ [[2, "y", "foo"]]
    2.times { j. tick }
    assert_equal([["bar"], ["foo"]], j.node.to_a.sort)
    assert_equal([[1, "x"]], j.sbuf.to_a.sort)

    j.seal_sbuf <+ [["..."]]
    2.times { j. tick }
    assert_equal([["bar"]], j.node.to_a.sort)
    assert_equal([[1, "x"]], j.sbuf.to_a.sort)

    j.res_approx <+ [[1, "x", "bar"]]
    2.times { j. tick }
    assert_equal([], j.node.to_a.sort)
    assert_equal([], j.sbuf.to_a.sort)
  end

  # Sealed collections don't need an explicit seal
  def test_join_rse_sealed
    j = JoinRseSealed.new
    j.sbuf <+ [[1, "a"], [2, "b"], [3, "c"]]
    2.times { j.tick }
    assert_equal([[1, "a"], [2, "b"], [3, "c"]], j.sbuf.to_a.sort)

    j.res_approx <+ [[1, "a", "bar"], [1, "a", "foo"], [2, "b", "bar"]]
    2.times { j.tick }
    assert_equal([[2, "b"], [3, "c"]], j.sbuf.to_a.sort)
  end

  def test_join_rse_negation_qual
    j = JoinRseNegationQual.new
    j.sbuf <+ [[1, "a"], [2, "b"], [3, "c"]]
    2.times { j.tick }
    assert_equal([[1, "a"], [2, "b"], [3, "c"]], j.sbuf.to_a.sort)

    j.res_approx <+ [["bar", 1, "x"], ["foo", 1, "x"], ["bar", 2, "x"]]
    2.times { j.tick }
    assert_equal([[2, "b"], [3, "c"]], j.sbuf.to_a.sort)
  end

  def test_join_rse_negation_qual_variant
    j = JoinRseNegationQualVariant.new
    j.sbuf <+ [[1, "a"], [2, "b"], [3, "c"]]
    2.times { j.tick }
    assert_equal([[1, "a"], [2, "b"], [3, "c"]], j.sbuf.to_a.sort)

    j.res_approx <+ [["x", "bar", 1], ["y", "foo", 1], ["z", "bar", 2]]
    2.times { j.tick }
    assert_equal([[3, "c"]], j.sbuf.to_a.sort)
  end

  def test_rse_join_sealed_twice
    j = JoinRseSealedUseTwice.new
    j.tick
  end

  def test_rse_join_twice
    j = JoinRseUseTwice.new
    j.tick
  end

  def test_rse_join_twice_reclaim_from_sealed
    j = JoinRseSealDoubleReclaim.new
    j.node <+ [["foo", "a", 1], ["bar", "a", 1],
               ["foo", "b", 1], ["bar", "c", 2]]
    j.tick

    j.x_log <+ [[100, "a"], [101, "b"]]
    j.tick

    assert_equal([["foo", "a", 1, 100, "a"],
                  ["foo", "b", 1, 101, "b"],
                  ["bar", "a", 1, 100, "a"]].sort,
                 j.x_res.to_a.sort)

    j.x_res_approx <+ [["foo", "a", 1, 100, "a"],
                       ["foo", "b", 1, 101, "b"]]
    2.times { j.tick }

    # x_log message 101 has been delivered to all the nodes in x_epoch "b" (just
    # "foo"); x_log message 100 hasn't been delivered to "bar" in x_epoch "a".
    assert_equal([[100, "a"]], j.x_log.to_a.sort)

    # There will be no more x_log messages in x_epoch "b" -- BUT, since there
    # might still be y_log messages in y_epoch 1, we can't GC the node fact for
    # x_epoch "b".
    j.seal_x_log_epoch <+ [["b"]]
    2.times { j.tick }

    assert_equal([["foo", "a", 1], ["bar", "a", 1],
                  ["foo", "b", 1], ["bar", "c", 2]].sort, j.node.to_a.sort)
  end

  def test_rse_join_double_scratch
    j = JoinRseDoubleScratch.new
    j.obj <+ [[5, "foo"], [10, "bar"]]
    j.ref <+ [[1, "x", 5], [2, "y", 5], [3, "z", 10]]
    2.times { j.tick }

    assert_equal([[5, "foo"], [10, "bar"]].to_set, j.obj.to_set)
    assert_equal([[1, "x", 5], [2, "y", 5], [3, "z", 10]].to_set, j.ref.to_set)

    j.seal_ref <+ [[true]]
    j.del_ref <+ [[100, 1], [101, 2]]
    2.times { j.tick }

    assert_equal([[10, "bar"]].to_set, j.obj.to_set)
    assert_equal([[3, "z", 10]].to_set, j.ref.to_set)
  end

  def test_rse_join_tlist_const
    j = JoinRseTlistConst.new
    j.b <+ [[5, 10], [6, 11]]
    j.c <+ [[7, 12]]
    j.a_approx <+ [[5, "foo", 12], [6, 99, 12]]
    2.times { j.tick }

    assert_equal([[5, 99, 12], [6, "foo", 12]].to_set, j.a.to_set)
    assert_equal([[5, 10], [6, 11]], j.b.to_a.sort)

    j.a_approx <+ [[5, 99, 12]]
    2.times { j.tick }

    assert_equal([[6, 11]], j.b.to_a.sort)
  end

  def test_rse_join_tlist_ip_port
    j = JoinRseTlistIpPort.new(:ip => "localhost", :port => 5555)
    j.b <+ [[5, 10], [6, 11]]
    j.c <+ [[7, 12]]
    j.a_approx <+ [[5, j.ip_port, j.port, 12]]
    2.times { j.tick }

    assert_equal([[6, j.ip_port, j.port, 12]].to_set, j.a.to_set)
    assert_equal([[6, 11]].to_set, j.b.to_set)
  end

  def test_rse_join_tlist_const_qual
    j = JoinRseTlistConstQual.new(:ip => "localhost", :port => 5556)
    j.b <+ [[5, 10], [6, 11]]
    j.c <+ [[7, 12]]
    j.a_approx <+ [[5, j.port, 100], [6, j.port + 1, 12]]
    2.times { j.tick }

    assert_equal([[6, j.port, 12]].to_set, j.a.to_set)
    assert_equal([[6, 11]].to_set, j.b.to_set)
  end

  # The test case uses an outer join, but should behave identically to an inner
  # join (i.e., we don't ever emit any NULL-padded join results).
  def test_rse_join_outer_check_sane
    j = JoinRseOuter.new
    j.t1 <+ [[5, 10], [6, 11]]
    j.t2 <+ [[5, 100], [6, 110]]
    2.times { j.tick }

    assert_equal([[10, 100], [11, 110]].to_set, j.r1.to_set)

    j.t3 <+ [[11, 110]]
    2.times { j.tick }

    assert_equal([[10, 100]].to_set, j.r1.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, j.t1.to_set)
    assert_equal([[5, 100], [6, 110]].to_set, j.t2.to_set)

    j.seal_t2_c <+ [[6]]
    2.times { j.tick }

    assert_equal([[10, 100]].to_set, j.r1.to_set)
    assert_equal([[5, 10]].to_set, j.t1.to_set)
    # XXX: Given that no more tuples with t2.c = 6 will be produced AND we've
    # already seen a matching t1 tuple (with a = 6), can we also reclaim the t2
    # tuple here? Given that it's an outer join, probably not (since a duplicate
    # copy of the t1 tuple would result in emitting a NULL-padded join result),
    # but perhaps for an inner join we could.
    assert_equal([[5, 100], [6, 110]].to_set, j.t2.to_set)

    j.t3 <+ [[10, 100]]
    2.times { j.tick }

    assert_equal([].to_set, j.r1.to_set)
    assert_equal([[5, 10]].to_set, j.t1.to_set)
    assert_equal([[5, 100], [6, 110]].to_set, j.t2.to_set)

    j.seal_t1_a <+ [[5]]
    2.times { j.tick }

    assert_equal([[5, 10]].to_set, j.t1.to_set)
    assert_equal([[6, 110]].to_set, j.t2.to_set)
  end

  def test_rse_join_outer
    j = JoinRseOuter.new
    j.t1 <+ [[5, 10], [6, 11]]
    j.t3 <+ [[11, nil]]
    2.times { j.tick }

    assert_equal([[10, nil]].to_set, j.r1.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, j.t1.to_set)

    j.seal_t2 <+ [[true]]
    2.times { j.tick }
    assert_equal([[10, nil]].to_set, j.r1.to_set)
    assert_equal([[5, 10]].to_set, j.t1.to_set)
  end

  def test_rse_join_notin_pullup
    j = JoinRseNotinPullup.new
    j.ref <+ [[1, "foo", 100], [2, "bar", 100], [3, "baz", 101]]
    j.obj <+ [[100, "xxx"], [101, "yyy"]]
    2.times { j.tick }

    assert_equal([["foo", 1, "xxx"], ["bar", 2, "xxx"], ["baz", 3, "yyy"]].to_set,
                 j.view.to_set)

    j.del_ref <+ [[20, 1], [21, 2]]
    2.times { j.tick }

    assert_equal([["baz", 3, "yyy"]].to_set, j.view.to_set)
    assert_equal([[101, "yyy"]].to_set, j.obj.to_set)
    assert_equal([[3, "baz", 101]].to_set, j.ref.to_set)
  end

  def test_rse_join_notin_pullup_implicit_qual
    j = JoinRseNotinPullupImplicitQual.new
    j.ref <+ [[1, "foo", 100], [2, "bar", 100], [3, "baz", 101]]
    j.obj <+ [[100, "xxx"], [101, "yyy"]]
    2.times { j.tick }

    assert_equal([[1], [2], [3]].to_set, j.live_ref.to_set)

    j.del_ref <+ [[1]]
    2.times { j.tick }

    assert_equal([[2], [3]].to_set, j.live_ref.to_set)
    assert_equal([[100, "xxx"], [101, "yyy"]].to_set, j.obj.to_set)
    assert_equal([[2, "bar", 100], [3, "baz", 101]].to_set, j.ref.to_set)
  end

  def test_rse_join_notin_pullup_implicit_qual_lefts
    j = JoinRseNotinPullupImplicitQualLefts.new
    j.ref <+ [[1, "foo", 100], [2, "bar", 100], [3, "baz", 101]]
    j.obj <+ [[100, "xxx"], [101, "yyy"]]
    2.times { j.tick }

    assert_equal([[1], [2], [3]].to_set, j.live_ref.to_set)

    j.del_ref <+ [[1]]
    2.times { j.tick }

    assert_equal([[2], [3]].to_set, j.live_ref.to_set)
    assert_equal([[100, "xxx"], [101, "yyy"]].to_set, j.obj.to_set)
    assert_equal([[2, "bar", 100], [3, "baz", 101]].to_set, j.ref.to_set)
  end

  def test_rse_join_recursion
    j = RseJoinRecursion.new
    j.tick
  end

  def test_rse_join_rhs
    skip        # XXX: doesn't work correctly yet
    j = RseJoinRhs.new
    j.safe_log <+ [[1, "k1", "k2"], [2, "k3", "k1"]]
    2.times { j.tick }

    assert_equal([[1]].to_set, j.dominated.to_set)
    assert_equal([[2, "k3", "k1"]].to_set, j.view.to_set)

    j.safe_log <+ [[3, "k2", "k4"]]
    2.times { j.tick }

    assert_equal([[1], [3]].to_set, j.dominated.to_set)
    assert_equal([[2, "k3", "k1"]].to_set, j.view.to_set)
  end

  def test_peter_bug
    p = PeterBug.new
    p.in1 <+ [[5]]
    p.in2 <+ [[5]]
    p.in3 <+ [[7]]
    3.times { p.tick }

    assert_equal([].to_set, p.out1.to_set)
    assert_equal([[5]].to_set, p.out3.to_set)
    assert_equal([[5]].to_set, p.in1.to_set)
  end

  def test_reclaim_lefts_no_block
    p = ReclaimLeftsNoBlock.new
    p.write <+ [[5, 10], [6, 11]]
    p.commit <+ [[10]]
    2.times { p.tick }

    assert_equal([[5, 10]].to_set, p.commit_event.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, p.write.to_set)

    p.write_log <+ [[5]]
    2.times { p.tick }

    assert_equal([].to_set, p.commit_event.to_set)
    assert_equal([[6, 11]].to_set, p.write.to_set)
  end

  def test_rse_from_range
    r = RseFromRange.new
    r.tick
  end
end

class ReclaimOuter
  include Bud

  state do
    table :x, [:x_key] => [:x_val]
    table :y, [:y_key] => [:y_val]
    table :y_copy, [:a, :b]
    scratch :z
    scratch :input_x
  end

  bloom do
    # Whenever there is an x-y match, we can reclaim both tuples.
    x <= input_x
    z <= x.notin(y, :x_key => :y_key)

    # We can copy y to a persistent table and still reclaim
    y_copy <= y {|t| [t.y_key + 1, t.y_val]}
  end
end

class ReclaimOuterIntersect
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :r1
    scratch :r2
  end

  bloom do
    # We can reclaim a t1 or t2 tuple when a matching t3 tuple appears. We can
    # only reclaim a t3 tuple when there are matching tuples in BOTH t1 and t2.
    r1 <= t1.notin(t3, :key => :key)
    r2 <= t2.notin(t3, :key => :key)
  end
end

class ReclaimOuterIntersectInverse
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :r1
    scratch :r2
  end

  bloom do
    # We can reclaim t1 tuples when a matching tuple appears in BOTH t2 and
    # t3. Now, you might think that we can reclaim a t2 or t3 tuple when a
    # matching tuple appears in t1, but that is incorrect: if we reclaimed a t2
    # tuple when its match appears in t1 (but before a match appears in t3), the
    # result would be to allow the t1 tuple to appear in the output of the
    # negation, which is incorrect.
    r1 <= t1.notin(t2, :key => :key)
    r2 <= t1.notin(t3, :key => :key)
  end
end

class ReclaimOuterInnerCombine
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    scratch :r1
    scratch :r2
  end

  bloom do
    # t2 appears on outer and can be reclaimed
    r1 <= t1.notin(t2, :key => :key)
    # t2 appears on inner and can be reclaimed; t3 cannot be reclaimed
    r2 <= t2.notin(t3)
  end
end

class ReclaimOuterPending
  include Bud

  state do
    scratch :r1
    scratch :input_t1
    table :t1
    table :t2
  end

  bloom do
    t1 <+ input_t1
    r1 <= t1.notin(t2, :key => :key)
  end
end

class ReclaimOuterIllegal
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
    table :t5
    table :t6
    table :t7
    table :t8
    table :t9
    table :t10
    table :t11
    scratch :r1
    scratch :r2
    scratch :r3
    scratch :r4
    scratch :r5
    scratch :r6
    scratch :s1
  end

  bloom do
    # Can't reclaim from t2 because it is used to produce an output collection
    r1 <= t1.notin(t2, :key => :key)
    r2 <= t2

    # Can't reclaim from t4 because it is used in aggregation
    r3 <= t3.notin(t4, :key => :key)
    t5 <= t4.group(nil, count)

    # Can't reclaim from t7 because it is used as the negative input to a notin
    # that is not a candidate for RSE
    r4 <= t6.notin(t7, :key => :key)
    t8 <= s1.notin(t7)

    # Can't reclaim from t11: usage in r5 rule is fine, usage in r6 rule is not
    r5 <= t9.notin(t11, :key => :key)
    r6 <= t10.notin(t11)
  end
end

class TestRseOuter < MiniTest::Unit::TestCase
  def test_outer_reclaim_simple
    r = ReclaimOuter.new
    r.input_x <+ [[5, 10], [6, 11]]
    r.y <+ [[6, 11], [100, 12], [150, 13]]
    3.times { r.tick }

    assert_equal([[5, 10]].to_set, r.z.to_set)
    assert_equal([[5, 10]].to_set, r.x.to_set)
    assert_equal([[100, 12], [150, 13]].to_set, r.y.to_set)
    assert_equal([[7, 11], [101, 12], [151, 13]].to_set, r.y_copy.to_set)

    # Ignore subsequent duplicate insertions into x
    r.input_x <+ [[6, 11], [7, 12]]
    3.times { r.tick }
    assert_equal([[5, 10], [7, 12]].to_set, r.z.to_set)
    assert_equal([[5, 10], [7, 12]].to_set, r.x.to_set)

    # Subsequent insertions into y (that match previously reclaimed x tuples)
    # should also be reclaimed (i.e., effectively ignored)
    r.y <+ [[6, 49]]
    3.times { r.tick }
    assert_equal([[5, 10], [7, 12]].to_set, r.z.to_set)
    assert_equal([[100, 12], [150, 13]].to_set, r.y.to_set)
    assert_equal([[7, 11], [7, 49], [101, 12], [151, 13]].to_set, r.y_copy.to_set)
  end

  def test_outer_reclaim_intersect
    r = ReclaimOuterIntersect.new
    r.t1 <+ [[5, 10], [6, 11]]
    r.t2 <+ [[6, 11], [7, 12]]
    r.t3 <+ [[7, 12]]
    3.times { r.tick }

    assert_equal([[5, 10], [6, 11]].to_set, r.r1.to_set)
    assert_equal([[6, 11]].to_set, r.r2.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.t1.to_set)
    assert_equal([[6, 11]].to_set, r.t2.to_set)
    assert_equal([[7, 12]].to_set, r.t3.to_set)

    # Key 7 now appears in both t1 and t2 => reclaim 7 from t3
    r.t1 <+ [[7, 13], [8, 14]]
    3.times { r.tick }

    assert_equal([[5, 10], [6, 11], [8, 14]].to_set, r.t1.to_set)
    assert_equal([].to_set, r.t3.to_set)

    # Subsequent duplicate insertions into t3 (that have previously been
    # reclaimed) should still be reclaimed
    r.t3 <+ [[7, 99]]
    3.times { r.tick }

    assert_equal([[5, 10], [6, 11], [8, 14]].to_set, r.t1.to_set)
    assert_equal([].to_set, r.t3.to_set)
  end

  def test_outer_reclaim_intersect_inverse
    r = ReclaimOuterIntersectInverse.new
    r.t1 <+ [[5, 10], [6, 11], [7, 12]]
    r.t2 <+ [[6, 99]]
    r.t3 <+ [[7, 99]]
    3.times { r.tick }

    assert_equal([[5, 10], [7, 12]].to_set, r.r1.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.r2.to_set)
    assert_equal([[5, 10], [6, 11], [7, 12]].to_set, r.t1.to_set)
    assert_equal([[6, 99]].to_set, r.t2.to_set)
    assert_equal([[7, 99]].to_set, r.t3.to_set)

    r.t2 <+ [[7, 120]]
    3.times { r.tick }
    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.r2.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.t1.to_set)
    assert_equal([[6, 99]].to_set, r.t2.to_set)
    assert_equal([].to_set, r.t3.to_set)

    r.t3 <+ [[6, 101]]
    3.times { r.tick }
    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10]].to_set, r.r2.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([].to_set, r.t2.to_set)
    assert_equal([].to_set, r.t3.to_set)

    # Subsequent duplicate insertions into t2/t3 (that have previously been
    # reclaimed) should still be reclaimed
    r.t2 <+ [[7, 121]]
    3.times { r.tick }
    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10]].to_set, r.r2.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([].to_set, r.t2.to_set)
    assert_equal([].to_set, r.t3.to_set)
  end

  def test_outer_inner_reclaim_combine
    r = ReclaimOuterInnerCombine.new
    r.t1 <+ [[5, 10], [6, 11]]
    r.t2 <+ [[6, 99], [7, 12]]
    r.t3 <+ [[7, 12], [8, 13]]
    3.times { r.tick }

    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[6, 99]].to_set, r.r2.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([[6, 99], [7, 12]].to_set, r.t2.to_set)
    assert_equal([[7, 12], [8, 13]].to_set, r.t3.to_set)

    r.t3 <+ [[6, 99]]
    3.times { r.tick }

    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([].to_set, r.r2.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([[7, 12]].to_set, r.t2.to_set)
    assert_equal([[6, 99], [7, 12], [8, 13]].to_set, r.t3.to_set)
  end

  def test_outer_reclaim_pending
    r = ReclaimOuterPending.new
    r.input_t1 <+ [[5, 10], [6, 11]]
    r.t2 <+ [[6, 11], [7, 12]]
    4.times { r.tick }

    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([[7, 12]].to_set, r.t2.to_set)

    r.input_t1 <+ [[6, 12]]
    4.times { r.tick }

    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([[7, 12]].to_set, r.t2.to_set)
  end

  def test_outer_reclaim_illegal
    r = ReclaimOuterIllegal.new
    r.t1 <+ [[5, 10], [6, 11]]
    r.t2 <+ [[6, 11], [7, 12]]
    r.t3 <+ [[5, 10], [6, 11]]
    r.t4 <+ [[6, 11], [7, 12]]
    r.t6 <+ [[5, 10], [6, 11]]
    r.t7 <+ [[6, 11], [7, 12]]
    r.t9 <+ [[5, 10], [6, 11]]
    r.t10 <+ [[5, 10], [6, 11]]
    r.t11 <+ [[6, 12], [7, 12]]
    3.times { r.tick }

    assert_equal([[5, 10]].to_set, r.r1.to_set)
    assert_equal([[5, 10]].to_set, r.t1.to_set)
    assert_equal([[6, 11], [7, 12]].to_set, r.r2.to_set)
    assert_equal([[6, 11], [7, 12]].to_set, r.t2.to_set)

    assert_equal([[5, 10]].to_set, r.r3.to_set)
    assert_equal([[5, 10]].to_set, r.t3.to_set)
    assert_equal([[6, 11], [7, 12]].to_set, r.t2.to_set)

    assert_equal([[5, 10]].to_set, r.r4.to_set)
    assert_equal([[5, 10]].to_set, r.t6.to_set)
    assert_equal([[6, 11], [7, 12]].to_set, r.t7.to_set)

    assert_equal([[5, 10]].to_set, r.r5.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.r6.to_set)
    assert_equal([[5, 10]].to_set, r.t9.to_set)
    assert_equal([[5, 10], [6, 11]].to_set, r.t10.to_set)
    assert_equal([[6, 12], [7, 12]].to_set, r.t11.to_set)
  end
end

class SealedCollection
  include Bud

  state do
    sealed :foo, [:x] => [:y]
    table :baz, foo.schema
  end

  bootstrap do
    foo <= [[5, 10], [6, 12]]
  end

  bloom do
    baz <= foo
  end
end

class TestSealed < MiniTest::Unit::TestCase
  def test_simple
    i = SealedCollection.new
    i.tick
    assert_equal([[5, 10], [6, 12]], i.foo.to_a.sort)
    assert_equal([[5, 10], [6, 12]], i.baz.to_a.sort)

    assert_raises(Bud::CompileError) do
      i.foo <+ [[7, 15]]
    end
  end
end

class ReliableBroadcast
  include Bud

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]
  end

  bloom do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end
end

class TestReliableDelivery < MiniTest::Unit::TestCase
  def test_space_basic
    opts = { :range_stats => false }
    ports = (1..3).map {|i| i + 10001}
    addrs = ports.map {|p| "127.0.0.1:#{p}"}
    rlist = ports.map {|p| ReliableBroadcast.new(opts.merge(:port => p))}
    rlist.each do |r|
      r.node <+ addrs.map {|a| [a]}
      r.tick
    end

    f = rlist.first
    id_buf = 0.upto(100).to_a.shuffle
    id_buf.each do |i|
      f.log <+ [[f.id(i), "message #{i}"]]
      f.tick
      if i % 10 == 0
        rlist.each(&:tick)
        sleep 0.1
      end
    end

    10.times { rlist.each(&:tick); sleep 0.1 }

    rlist.each do |r|
      assert_equal(3, r.chn_approx.physical_size)
    end
    rlist.each(&:stop)
  end
end
