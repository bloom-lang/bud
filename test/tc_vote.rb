require 'rubygems'
require 'bud'
require 'test/unit'

require 'examples/simple_broadcast'
require 'examples/vote'



class VoteUser < Vote
  def state
    super
  end
  def declaration 
    super
    strata[2] = rules {
      vote <+ deliver.map{|d| [d.message, 'Y'] }
      j2 = join [mcnt, vcnt]
      status <+ j2.map do |m, c|
        if m.cnt == c.cnt and c.vote == "Y"
          [c.ballot, "Success"]
        end
      end
    }
  end
end

class TestAggs < Test::Unit::TestCase
  def test_vote
    v = VoteUser.new("127.0.0.1", 10001)
    v.tick
    v.run_bg
    v.member << ['127.0.0.1:10001']

    v.ballot <+ [['foobar']]


    assert_nothing_raised(RuntimeError) {v.tickle}
    assert_nothing_raised(RuntimeError) {v.tickle}
    assert_nothing_raised(RuntimeError) {v.tickle}

    #v.status.each {|s| print "S: #{s.inspect}\n"; } 
    v.status.each do |s| 
      print "S: #{s.inspect}\n"
      assert_equals(s.ballot, 'foobar')
      assert_equals(s.status, 'Success')
    end

  end

end
