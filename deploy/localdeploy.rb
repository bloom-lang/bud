require 'rubygems'
require 'bud'

# Starts up a bunch of Bud instances locally on 127.0.0.1, with ephemoral ports.
# This is for the case where you just want to test stuff locally, but you don't
# really care about port numbers.
module LocalDeploy

  include BudModule

  state {
    table :node_count, [] => [:num]
  }

  declare
  def rules
    deploy_node <= (1..node_count[[]].num).map{ |i|
      if idempotent [[:node, i]]
        # XXX: ugly hack because we pull out assignment expressions from rules
        foo = nil
        eval 'foo = MetaRecv.new(:ip => "127.0.0.1")'
        # end ugly hack
        foo.run_bg
        [i, "127.0.0.1:" + foo.port.to_s]
      end
    }
  end

end
