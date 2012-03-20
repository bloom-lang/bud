require './test_common'

class BasicTemp
  include Bud

  state do
    scratch :out, [:val]
    scratch :inski
  end

  bloom :rules do
    temp :tmpy <= inski
    out <= tmpy {|t| [t.val]}
  end
end

class DupTemp < BasicTemp
  bloom :rules do
    temp :tmpy <= inski
    temp :tmpy <= inski
    out <= tmpy {|t| [t.val]}
  end
end

class ReuseTemp < BasicTemp
  bloom :rules do
    temp :hemp <= inski
    hemp <= [[1,2]]
  end
end

class TempNext
  include Bud

  state do
    scratch :inski, [:c1, :c2] => [:c3, :c4]
    scratch :wait, [:c1, :c2] => [:c3, :c4]
    scratch :out, [:c1, :c2] => [:c3, :c4]
  end

  bloom do
    temp :tmpy <= inski
    temp :waity <+ wait
    out <= tmpy
    out <= waity
  end
end

class TempNoSchema
  include Bud

  bloom do
    temp :out <= [[1,2], [3, 4]]
  end
end

class SimpleTempTest
  include Bud

  state do
    table :t1
    table :t2
    scratch :t3
    scratch :t4
    scratch :t5
  end

  bootstrap do
    t1 << [5, 10]
    t1 << [10, 20]
    t2 << [50, 100]
    t2 << [75, 125]
  end

  bloom do
    temp :k <= (t1 * t2)
    t3 <= k.map {|a, b| [a.key + b.key, a.val + b.val]}
    t4 <= k.map {|a, b| [a.key + b.key, a.val + b.val]}
    t5 <= k.map {|a, b| a if b.val > 100}
  end
end

class TempRefTemp
  include Bud

  state do
    table :t1
    scratch :t2
  end

  bootstrap do
    t1 << [100, 200]
  end

  bloom do
    temp :a <= t1.map {|t| [t.key + 10, t.val + 10]}
    temp :b <= a.map {|t| [t[0] + 20, t[1] + 20]}
    temp :c <= b.map {|t| [t[0] - 50, t[1] - 100]}
    temp :d <= b.map {|t| [t[0] - 50, t[1] - 100]} # unreferenced
    t2 <= c
  end
end

class TempShadow
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
  end

  bootstrap do
    t1 << [20,40]
    t1 << [40,60]
  end

  bloom do
    temp :k <= t1.map {|t| [t.key + 10, t.val + 20]}
    t2 <= k
    t2 <= t1.map {|k| [k.key, k.val]}
    t3 <= (t1 * t2).pairs(:key => :key) {|j,k| [j.key + 20, k.val + 20]}
    t4 <= t3.map {|t| [t.key, t.val] unless k.include? t}
  end
end

class SimpleTempNoMapTest
  include Bud

  state do
    table :t1
    table :t2
    scratch :t3
    scratch :t4
    scratch :t5
  end

  bootstrap do
    t1 << [5, 10]
    t1 << [10, 20]
    t2 << [50, 100]
    t2 << [75, 125]
  end

  bloom do
    temp :k <= (t1 * t2)
    t3 <= k {|a, b| [a.key + b.key, a.val + b.val]}
    t4 <= k {|a, b| [a.key + b.key, a.val + b.val]}
    t5 <= k {|a, b| a if b.val > 100}
  end
end

class TempNoMapRefTempNoMap
  include Bud

  state do
    table :t1
    scratch :t2
  end

  bootstrap do
    t1 << [100, 200]
  end

  bloom do
    temp :a <= t1 {|t| [t.key + 10, t.val + 10]}
    temp :b <= a {|t| [t[0] + 20, t[1] + 20]}
    temp :c <= b {|t| [t[0] - 50, t[1] - 100]}
    temp :d <= b {|t| [t[0] - 50, t[1] - 100]} # unreferenced
    t2 <= c
  end
end

class TempNoMapShadow
  include Bud

  state do
    table :t1
    table :t2
    table :t3
    table :t4
  end

  bootstrap do
    t1 << [20,40]
    t1 << [40,60]
  end

  bloom do
    temp :k <= t1 {|t| [t.key + 10, t.val + 20]}
    t2 <= k
    t2 <= t1 {|k| [k.key, k.val]}
    t3 <= (t1 * t2).pairs(:key => :key) {|j,k| [j.key + 20, k.val + 20]}
    t4 <= t3 {|t| [t.key, t.val] unless k.include? t}
  end
end

# Check that schema inference works when many of the initial inputs are nil
class TempWithPredicate
  include Bud

  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [1, 11]
    t1 << [2, 11]
    t1 << [3, 11]
    t1 << [4, 11]
    t1 << [5, 11]
    t1 << [6, 11]
    t1 << [7, 11]
  end

  bloom do
    temp :xyz <= t1 {|t| t if t.key == 7}
    t2 <= xyz
  end
end

# Check that schema inference works for <+ rules
class TempAtNext
  include Bud

  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [5, 10]
  end

  bloom do
    temp :xyz <+ t1 {|t| [t.key + 1, t.val + 1]}
    t2 <= xyz
  end
end

class TestTemps < MiniTest::Unit::TestCase
  def test_basic_temp
    p = BasicTemp.new
    p.run_bg
    p.sync_do{p.inski <+ [[1,1], [2,2], [3,3]]}
    p.stop
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.val]}.sort)
  end
  def test_retemp
    p = BasicTemp.new
    p.run_bg
    p.sync_do{p.inski <+ [[1,1], [2,2], [3,3]]}
    p.stop
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.val]}.sort)
  end
  def test_temp_next
    p = TempNext.new
    p.run_bg
    p.sync_do do 
      p.inski <+ [[1,1,2,2],
                  [2,2,3,3],
                  [3,3,4,4]]
      p.wait <+  [[5,5,6,6],
                  [7,7,8,8],
                  [9,9,9,9]]
    end
    assert_equal(3, p.out.length)
    assert_equal([[1], [2], [3]], p.out.map{|o| [o.c1]}.sort)
    p.sync_do
    assert_equal(3, p.out.length)
    assert_equal([[5], [7], [9]], p.out.map{|o| [o.c1]}.sort)
    p.stop
  end
  def test_dup_tmp
    assert_raises(Bud::CompileError) {DupTemp.new}
  end
  def test_reuse_tmp
    p = ReuseTemp.new
    p.tick
    assert_equal(1, p.hemp.length)
  end
  def test_no_schema
    p = TempNoSchema.new
    p.tick
  end
  def test_simple_temp
    p = SimpleTempTest.new
    p.tick
    assert_equal(p.t3.to_a.sort, [[55, 110], [60, 120], [80, 135], [85, 145]])
    assert_equal(p.t4.to_a.sort, [[55, 110], [60, 120], [80, 135], [85, 145]])
    assert_equal(p.t5.to_a.sort, [[5, 10], [10, 20]])
  end

  def test_temp_pred_schema_infer
    p = TempWithPredicate.new
    p.tick
    assert_equal(p.t2.to_a.sort, [[7, 11]])
  end

  def test_test_atnext_schema_infer
    p = TempAtNext.new
    p.tick
    assert_equal(p.t2.to_a.sort, [])
    p.tick
    assert_equal(p.t2.to_a.sort, [[6, 11]])
  end

  def test_temp_in_temp
    p = TempRefTemp.new
    p.tick
    assert_equal(p.t2.to_a.sort, [[80, 130]])
  end

  def test_temp_shadow
    p = TempShadow.new
    p.tick
    assert_equal(p.k.to_a.sort, [[30, 60], [50, 80]])
    assert_equal(p.t2.to_a.sort, [[20, 40], [30, 60], [40, 60], [50, 80]])
    assert_equal(p.t3.to_a.sort, [[40, 60], [60, 80]])
    assert_equal(p.t3.to_a.sort, p.t4.to_a.sort)
  end

  class Issue132
    include Bud

    state do
      table :foo
      table :bar
    end

    bootstrap do
      foo << [10, 10]
    end

    bloom do
      temp :baz <= foo do |f|
        [f.key, f.val + 1]
      end
      bar <= baz
    end
  end

  def test_issue132
    i = Issue132.new
    i.tick
    assert_equal(i.bar.to_a.sort, [[10, 11]])
  end
end

class TestTempNoMaps < MiniTest::Unit::TestCase
  def test_simple_test_nomap
    p = SimpleTempNoMapTest.new
    p.tick
    assert_equal(p.t3.to_a.sort, [[55, 110], [60, 120], [80, 135], [85, 145]])
    assert_equal(p.t4.to_a.sort, [[55, 110], [60, 120], [80, 135], [85, 145]])
    assert_equal(p.t5.to_a.sort, [[5, 10], [10, 20]])
  end

  def test_temp_nomap_in_temp_nomap
    p = TempNoMapRefTempNoMap.new
    p.tick
    assert_equal(p.t2.to_a.sort, [[80, 130]])
  end

  def test_temp_nomap_shadow
    p = TempNoMapShadow.new
    p.tick
    assert_equal(p.k.to_a.sort, [[30, 60], [50, 80]])
    assert_equal(p.t2.to_a.sort, [[20, 40], [30, 60], [40, 60], [50, 80]])
    assert_equal(p.t3.to_a.sort, [[40, 60], [60, 80]])
    assert_equal(p.t3.to_a.sort, p.t4.to_a.sort)
  end
end

module TestDefModule
  state do
    table :t1
    table :t2
  end

  bootstrap do
    t1 << [10, 10]
  end

  bloom do
    temp :t3 <= t1 {|t| [t.key + 10, t.val + 20]}
    t2 <= t3 {|t| [t[0] + 20, t[1] + 40]}
  end
end

class TestModuleUser
  include Bud
  include TestDefModule
end

class TestModuleTemp < MiniTest::Unit::TestCase
  def test_simple
    c = TestModuleUser.new
    c.tick
    assert_equal(c.t2.to_a.sort, [[40, 70]])
  end
end
