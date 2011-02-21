module Bud
  ######## error types  
  class BudError < Exception
  end

  class BudTypeError < BudError
  end
  
  class KeyConstraintError < BudError
  end
end
