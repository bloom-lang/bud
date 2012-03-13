require './test_common'

class Nesting
  include Bud

  state do
    table :nested_people, [:p_id, :firstname, :lastname, :hobbies]
    table :has_hobby, [:person_id, :name]
    table :meta, [:name, :tab]
    scratch :flat, [:p_id, :firstname, :lastname, :hobby]
    scratch :renested, nested_people.key_cols => nested_people.val_cols
    scratch :np2, [:firstname, :lastname, :hobbies]
  end

  bootstrap do
    nested_people <= [[1, 'Nick', 'Machiavelli', ['scheming', 'books']]]
    nested_people <= [[2, 'Chris', 'Columbus', ['sailing', 'books']]]
    has_hobby <= [[1, 'scheming'], [1, 'books'], [2, 'sailing'], [2, 'books']]
    meta <= [["nested_people", nested_people], ["has_hobby", has_hobby]]
  end

  bloom :simple_nesting do
    flat <= nested_people.flat_map do |p|
      p.hobbies.map { |h| [p.p_id, p.firstname, p.lastname, h] }
    end
  end

  bloom :simple_renest do
    renested <= flat.group([flat.p_id, flat.firstname, flat.lastname], accum(flat.hobby))
  end

  bloom :structured_nesting do
    np2 <= meta.flat_map do |m|
      m.tab.map {|t| [t.firstname, t.lastname, t.hobbies] if m.name == 'nested_people'}
    end
  end
end

class TestNest < MiniTest::Unit::TestCase
  def test_nest
    u = Nesting.new
    u.tick
    assert_equal([[1, "Nick", "Machiavelli", "books"],
                  [1, "Nick", "Machiavelli", "scheming"],
                  [2, "Chris", "Columbus", "books"],
                  [2, "Chris", "Columbus", "sailing"]].sort,
                 u.flat.to_a.sort)

    a = u.renested.map{|t| [t[0], t[1], t[2], t[3].sort]}
    assert_equal([[1, "Nick", "Machiavelli", ["books", "scheming"]],
                  [2, "Chris", "Columbus", ["books", "sailing"]]].sort,
                 a.sort)
    assert_equal([["Nick", "Machiavelli", ["scheming","books"]],
                  ["Chris", "Columbus", ["sailing", "books"]]].sort,
                 u.np2.to_a.sort)
  end
end
