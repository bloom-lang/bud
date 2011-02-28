module Bud
  ######## error types  
  class BudError < Exception;  end
  class BootstrapError < BudError;  end
  class BudTypeError < BudError;  end
  class KeyConstraintError < BudError;  end
  class CompileError < BudError; end
end
