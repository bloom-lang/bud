require './test_common'

class WinMove
  include Bud

  state do
    poset :move, [:from, :to]
    table :win, [:pos]
  end

  bloom do
    win <= move.notin(win, :to => :pos).pro {|m| [m.from]}
  end
end

class TestWinMove < MiniTest::Unit::TestCase
  def test_win_move_unstrat
    assert_raises(Bud::CompileError) { WinMove.new }
  end

  def test_win_move_1
    w = WinMove.new(:stratum_map => {
                      "move" => 0,
                      "win" => 0
                      })
    w.move <+ [["A", "B"],
               ["B", "C"]]
    w.tick

    assert_equal([["B"]].to_set, w.win.to_set)
  end

  # Two disconnected graphs
  def test_win_move_2
    w = WinMove.new(:stratum_map => {
                      "move" => 0,
                      "win" => 0
                      })
    w.move <+ [["A", "B"],
               ["B", "C"],
               ["X", "Y"],
               ["Y", "Z"]]
    w.tick

    assert_equal([["B"], ["Y"]].to_set, w.win.to_set)
  end

  # Positions reachable from multiple squares (B -> A, C -> A, D -> A).
  def test_win_move_3
    w = WinMove.new(:stratum_map => {
                      "move" => 0,
                      "win" => 0
                      })
    w.move <+ [["B", "A"],
               ["C", "A"],
               ["D", "A"],
               ["E", "B"],
               ["F", "E"],
               ["G", "F"],
               ["H", "G"]]
    w.tick

    assert_equal([["B"], ["F"], ["H"], ["C"], ["D"]].to_set, w.win.to_set)
  end

  # Moves that are transitively redundant (B -> A, C -> B, C -> A)
  def test_win_move_4
    w = WinMove.new(:stratum_map => {
                      "move" => 0,
                      "win" => 0
                      })
    w.move <+ [["B", "A"],
               ["C", "B"],
               ["C", "A"]]
    w.tick

    assert_equal([["B"], ["C"]].to_set, w.win.to_set)
  end
end

class PartHierarchy
  include Bud

  state do
    poset :part, [:id, :child]
    table :tested, [:id]
    scratch :working, [:id]
    scratch :has_suspect_part, [:id]
  end

  bloom do
    working <= tested
    working <= part {|p| [p.id]}.notin(has_suspect_part)
    has_suspect_part <= part.notin(working, :child => :id).pro {|p| [p.id]}
  end
end

class TestPartHierarchy < MiniTest::Unit::TestCase
  def test_part_hierarchy_unstrat
    assert_raises(Bud::CompileError) { PartHierarchy.new }
  end

  def test_part_hierarchy_manual_strat
    p = PartHierarchy.new(:stratum_map => {
                            "tested" => 0, "working" => 1,
                            "has_suspect_part" => 1, "part" => 0
                          })
    p.part <+ [["house", "kitchen"],
               ["house", "garage"],
               ["house", "bedroom"]]
    p.tick

    puts "WORKING: #{p.working.to_a.sort.inspect}"
    puts "SUSPECT_PART: #{p.has_suspect_part.to_a.sort.inspect}"
  end
end

class PosetSimple
  include Bud

  state do
    poset :t1, [:x, :y]
    table :t2, t1.schema
  end

  bloom do
    t2 <= t1 {|t| [t.x + 1, t.y + 2]}
  end
end

class PosetKeys
  include Bud

  state do
    poset :t1, [:x] => [:y]
    table :t2, [:x, :y]
  end

  bloom do
    t1 <= t2 {|t| [t.x + 10, t.y + 12]}
  end
end

class TestPoset < MiniTest::Unit::TestCase
  def test_poset_simple
    t = PosetSimple.new
    t.t1 <+ [[5, 1], [5, 2], [10, 5]]
    t.tick

    assert_equal([[6, 3], [6, 4], [11, 7]].to_set, t.t2.to_set)
  end

  def test_poset_keys1
    t = PosetKeys.new
    assert_raises(Bud::KeyConstraintError) { t.t1 <+ [[5, 10], [5, 11]] }
  end

  def test_poset_keys2
    t = PosetKeys.new
    t.t1 <+ [[15, 14]]
    t.tick

    # Allow duplicates
    t.t2 <+ [[5, 2]]
    t.tick
    assert_equal([[15, 14]], t.t1.to_a.sort)

    t.t2 <+ [[5, 11]]
    assert_raises(Bud::KeyConstraintError) { t.tick }
  end
end
