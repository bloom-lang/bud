module Bud
  # Root Bud exception type.
  class Error < StandardError; end

  # Raised (at runtime) when a type mismatch occurs (e.g., supplying a
  # non-Enumerable object to the RHS of a Bud statement).
  class TypeError < Error; end

  # Raised when a primary key constraint is violated.
  class KeyConstraintError < Error; end

  # Raised when the input program fails to compile (e.g., due to illegal
  # syntax).
  class CompileError < Error; end

  # Raised when the program is given in an illegal location (e.g., presented as
  # an eval block).
  class IllegalSourceError < Error; end
  
  # Raised when evaluation halts with outstanding callbacks.
  class ShutdownWithCallbacksError < Error; end
end
