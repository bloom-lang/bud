# test for th

require 'rubygems'
require 'bud'
require 'test/unit'
require 'examples/cart'


class TestBud < Bud
  def state
    # a "normal", overwriteable variable
    blackboard :myvar, [], ['data']
    # a committed choice variable
    permanent :log, [], ['data']
    # otherwise, the constraint is protected by an exception
    table :regular, [], ['data']

    # an overwritable "struct"
    blackboard :struct_o, [], ['left', 'right', 'middle']

    # a table of variables
    blackboard :kv_o, ['key'], ['value']
    # its counterpart of "finals"
    permanent :kv_c, ['key'], ['value']

    table :inp, ['data']
  end

  def declaration
    myvar <= inp.map{|i| i}
  end

end

class TestConflicts < Test::Unit::TestCase
  def test_conf1
    t = TestBud.new("localhost", 10001)

    t.tick
    t.myvar << ['one']
    t.myvar << ['two']
    t.myvar.each{|r| assert_equal('two', r.data) }

    t.log << ['one']
    t.log << ['two']
    t.log << ['three']
    t.log.each{|r| assert_equal('one', r.data) }

    
    t.regular << ['one']
    assert_raise(Bud::KeyConstraintError) {t.regular << ['two']}

    t.kv_o << ['sign', 'signified']
    t.kv_o << ['sign', 'signer']
    t.kv_o << ['sign', 'stop']

    t.kv_o.each{|k| assert_equal("stop", k.value)}

    t.kv_c << ['sign', 'signified']
    t.kv_c << ['sign', 'signer']
    t.kv_c << ['sign', 'stop']

    t.kv_c.each{|k| assert_equal("signified", k.value)}

    t.inp << ['foo']
    t.inp << ['bar']
    t.inp << ['baz']
    t.tick
    assert_equal(1, t.myvar.length)
  end
  

end
