module Bud
  # Root Bud exception type.
  class BudError < StandardError; end

  # Raised (at runtime) when a type mismatch occurs (e.g., supplying a
  # non-Enumerable object to the RHS of a Bud statement).
  class BudTypeError < BudError; end

  # Raised when a primary key constraint is violated.
  class KeyConstraintError < BudError; end

  # Raised when the input program fails to compile (e.g., due to illegal
  # syntax).
  class CompileError < BudError; end
end
