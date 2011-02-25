module Bud
  class BudError < Exception; end
  class KeyConstraintError < BudError; end
  class CompileError < BudError; end
end
