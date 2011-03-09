require 'test_common'

module ParentModule
  include BudModule

  state do
    table :t1
  end

  declare
  def do_parent
  end
end

module ChildModule
  import ParentModule => :p

  state do
    table :t2
  end

  declare
  def do_child
    t2 <= p.t1.map {|t| [t.key + 1, t.val + 1]}
  end
end
