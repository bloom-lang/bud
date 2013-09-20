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

class RseDoubleNeg
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
  end

  bloom do
    t1 <= t2.notin(t3).notin(t4)
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

    # Reference as the outer (NM) operand to a notin
    res <= t7.notin(t8)
    t9 <= t10.notin(t7)

    # Dataflow reaches both a persistent and a transient-output collection. We
    # don't want to delete from t7 in this circumstance, because we regard the
    # content of "output" as needing to be preserved.
    res <= t11.notin(t12)
    t9 <= t11
    out <= t11
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
    res <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [n.addr] + s}).notin(res_approx)
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
    res <= ((sbuf * node).pairs(node.epoch => sbuf.epoch) {|s,n| [n.addr] + s}).notin(res_approx)
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

  def test_rse_double_neg
    s = RseDoubleNeg.new
    s.t2 <+ [[1, 1], [2, 2], [3, 3]]
    s.t3 <+ [[2, 2]]
    s.t4 <+ [[3, 3]]
    2.times { s.tick }

    assert_equal([[1, 1]], s.t1.to_a.sort)
    assert_equal([[1, 1]], s.t2.to_a.sort)
  end

  def test_rse_delete_downstream
    s = RseDeleteDownstream.new
    s.t2 <+ [[5, 10], [6, 11]]
    s.t3 <+ [[5, 10]]
    2.times { s.tick }

    assert_equal([[6, 11]], s.t2.to_a.sort)
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
    2.times { s.tick }

    assert_equal([[1, 1], [2, 2]], s.t1.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t4.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t7.to_a.sort)
    assert_equal([[1, 1], [2, 2]], s.t11.to_a.sort)
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
