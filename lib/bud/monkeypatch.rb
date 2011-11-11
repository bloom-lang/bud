# We monkeypatch Module to add support for Bloom state and code declarations.
require 'bud/source'

class Class
  def modules
    a = self.ancestors
    a[1..a.index(superclass)-1]
  end
end


$moduleWrapper = {} # module => wrapper class.  See import below.
class Module
  def modules
    ancestors[1..-1]
  end

  # import another module and assign to a qualifier symbol: <tt>import MyModule => :m</tt>
  def import(spec)
    raise Bud::CompileError unless (spec.class <= Hash and spec.length == 1)
    mod, local_name = spec.first
    raise Bud::CompileError unless (mod.class <= Module and local_name.class <= Symbol)

    # A statement like this:
    #   import MyModule => :m
    # is translated as follows. First, module MyModule is made instantiable by wrapping it in a class
    #   class MyModule__wrap__
    #     include Bud
    #     include MyModule
    #   end
    #
    # Then introduce a method "m", the import binding name, in the calling module/class
    # (the one with the import statement). This returns an instance of the wrapped class.
    #   inst = MyModule__wrap__.new
    #   def m
    #      inst
    #   end

    mod, local_name = spec.first
    if self.instance_methods.include? local_name.to_s or self.instance_methods.include? local_name
      filename, num = caller(0)
      raise "#{local_name} is already taken."
    end

    unless $moduleWrapper[mod]
      $moduleWrapper[mod] = eval %{
        class #{mod}__wrap__
          include Bud
          include #{mod}
        end
      }
    end

    # inst = MyModule__wrap__.new
    klass = $moduleWrapper[mod]
    inst = klass.new

    # add "def m" method to calling module or class
    self.class_eval do
      define_method(local_name) do
        inst
      end
    end
  end

  # the block of Bloom collection declarations.  one per module.
  def state(&block)
    meth_name = Module.make_state_meth_name(self)
    define_method(meth_name, &block)
  end

  # a ruby block to be run before timestep 1.  one per module.
  def bootstrap(&block)
    meth_name = "__bootstrap__#{Module.get_class_name(self)}".to_sym
    define_method(meth_name, &block)
  end

  # bloom statements to be registered with Bud runtime.  optional +block_name+ 
  # allows for multiple bloom blocks per module, and overriding
  def bloom(block_name=nil, &block)
    # If no block name was specified, generate a unique name
    if block_name.nil?
      @block_id ||= 0
      block_name = "#{Module.get_class_name(self)}__#{@block_id.to_s}"
      @block_id += 1
    else
      unless block_name.class <= Symbol
        raise Bud::CompileError, "Bloom block names must be a symbol: #{block_name}"
      end
    end

    # Note that we don't encode the module name ("self") into the name of the
    # method. This allows named blocks to be overridden (via inheritance or
    # mixin) in the same way as normal Ruby methods.
    meth_name = "__bloom__#{block_name}"

    # Don't allow duplicate named bloom blocks to be defined within a single
    # module; this indicates a likely programmer error.
    if instance_methods(false).include? meth_name
      raise Bud::CompileError, "Duplicate named bloom block: '#{block_name}' in #{self}"
    end
    ast = Source.read_block(caller[0]) # pass in caller's location via backtrace
    # ast corresponds only to the statements of the block. Wrap it in a method
    # definition for backward compatibility for now.
    # First wrap ast in a block if it is only a single statement
    ast = s(:block, ast) unless ast.sexp_type == :block
    ast = s(:defn, meth_name.to_sym, s(:args), s(:scope, ast))
    unless self.respond_to? :__bloom_asts__
      def self.__bloom_asts__;
        @__bloom_asts__ ||= {}
        @__bloom_asts__
      end
    end
    __bloom_asts__[meth_name] = ast
    define_method(meth_name.to_sym, &block)
  end

  private
  # Return a string with a version of the class name appropriate for embedding
  # into a method name. Annoyingly, if you define class X nested inside
  # class/module Y, X's class name is the string "Y::X". We don't want to define
  # method names with semicolons in them, so just return "X" instead.
  def self.get_class_name(klass)
    klass.name.split("::").last
  end

  # State method blocks are named using an auto-incrementing counter. This is to
  # ensure that we can rediscover the possible dependencies between these blocks
  # after module import (see Bud#call_state_methods).
  def self.make_state_meth_name(klass)
    @state_meth_id ||= 0
    r = "__state#{@state_meth_id}__#{Module.get_class_name(klass)}".to_sym
    @state_meth_id += 1
    return r
  end
end
