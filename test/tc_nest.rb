require 'test_common'
require 'backports'

class Nesting < Bud
  def state
    table :nested_people, ['p_id', 'firstname', 'lastname', 'hobbies']
    table :has_hobby, ['person_id', 'name']
    table :meta, ['name', 'tab']
    scratch :flat, ['p_id', 'firstname', 'lastname', 'hobby']
    scratch :renested, nested_people.keys, nested_people.cols
    scratch :np2, ['firstname', 'lastname', 'hobbies']
  end

  def bootstrap
    nested_people <= [[1, 'Nick', 'Machiavelli', ['scheming', 'books']]]
    nested_people <= [[2, 'Chris', 'Columbus', ['sailing', 'books']]]
    has_hobby <= [[1, 'scheming'], [1, 'books'], [2, 'sailing'], [2, 'books']]
    meta <= [["nested_people", nested_people], ["has_hobby", has_hobby]]
  end
  
  declare
  def simple_nesting
    flat <= nested_people.flat_map do |p|
      p.hobbies.map { |h| [p.p_id, p.firstname, p.lastname, h] }
    end
  end
  
  declare
  def simple_renest
    renested <= flat.group([flat.p_id, flat.firstname, flat.lastname], accum(flat.hobby))
  end
  
  declare
  def structured_nesting
    np2 <= meta.flat_map do |m|
      m.tab.map{ |t| [t.firstname, t.lastname, t.hobbies] if m.name == 'nested_people'}
    end
  end
end

class TestNest < Test::Unit::TestCase
  def test_nest
    u = Nesting.new
    u.tick
    assert_equal([[1, "Nick", "Machiavelli", "books"],
                  [1, "Nick", "Machiavelli", "scheming"],
                  [2, "Chris", "Columbus", "books"],
                  [2, "Chris", "Columbus", "sailing"]].sort, 
                 u.flat.map{|t| t}.sort)
    assert_equal([[1, "Nick", "Machiavelli", ["scheming", "books"]],
                  [2, "Chris", "Columbus", ["books", "sailing"]]].sort,
                 u.renested.map{|t| t}.sort)
    assert_equal([["Nick", "Machiavelli", ["scheming","books"]], 
                  ["Chris", "Columbus", ["sailing", "books"]]].sort, 
                 u.np2.map{|t| t}.sort)
  end
end
