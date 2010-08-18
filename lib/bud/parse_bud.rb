# extract the dependency relation among collections from Bud code.
# TODO: merge with the other parser and do both in a single "pass."

require 'rubygems'
require 'parse_tree'
require 'sexp_path'
#require 'lib/bud/meta.rb'
require 'ruby2ruby'

class BudMirror < SexpProcessor
  attr_reader :depends
  def initialize
    super
    self.warn_on_default = false
    self.strict = false
    # collection operators
    @sups = Hash['<=', 1, '<+', 1, '<-', 1]
    # functions indicating nonmonotonic operations
    @nonmono = Hash['argagg', 1, 'group', 1, 'reduce', 1]
    @depends = Array.new
  end

  def each_depends
    @depends.each do |d|
      d.each do |sub|
        yield sub
      end
    end
  end

  # processor helper functions.
  # process_CALL overrides the default Sexp processor for CALL
  # for simplicity, this is a side-effecting, impure parse,
  # because all we care about for now is dependencies.
  # that is, we do not attempt to reduce the Sexp to a final form --
  # just enough to extract dependency info

  def process_dvar(exp)
    exp.shift 
    return s(exp.shift.to_s)
  end

  def process_dasgn_curr(exp)
    exp.shift
    # we only care about curr assignments if there is an rhs
    # e.g. x, not z or y, in
    # x = join [...]
    # [....].map |y, z|
    if exp.length == 2
      l = exp.shift
      o = process exp.shift
      @depends.push DedRule.new(l.to_s, "<=", o)
      return s(l.to_s + "=" + o.to_s)
    else 
      # dis(c|reg)ard
      exp.shift
      return s()
    end
  end

  def process_join(exp)
    j = exp.shift
    # we are only interested in the joined tables, not the quals
    tabs = j[0]
    return tabs
  end

  def process_fcall(exp)
    l = exp.shift
    r = exp.shift

    # note: deal with 'join assignments'
    if r.to_s == "rules" 
      return s()
    elsif r.to_s == "join"
      rhs = process exp.shift
      return process_join(rhs)
    else
      rhs = process exp.shift
      return s()
    end
  end

  def process_array(exp)
    exp.shift
    rets = Array.new
    while exp.length > 0
      l = process exp.shift
      rets.push(l)
    end
    return rets.length == 0 ? s() : rets.length == 1 ? rets[0] : s(rets)
  end

  def process_iter(exp)
    exp.shift
    l = process exp.shift
    o = process exp.shift
    r = process exp.shift

    #print "\titer: l=#{l}, o=#{o}, r=#{r}\n"
    # the current analysis only cares about dependencies between tables.
    # if we're iterating over something, that something is a table in the rhs
    # of a rule.  forget the rest (for now).  There is one exception: the rules
    # block, which appears in the rhs of an iter.
 
    if l.to_s == "s()" 
      return r
    else
      return l
    end
  end

  def process_vcall(exp)
   exp.shift
   #print "vcall is #{exp.shift} and now exp has #{exp.length}\n"
   vc = exp.shift
   if vc.class == Sexp
     return vc
   else 
     return s(vc)
   end
  end

  def process_call(exp)
    # superator 'assignments' are 'calls' in the AST.  
    # the lhs and rhs should already be processed.
    l = exp.shift
    name = process exp.shift
    argsxp = exp.shift
    if argsxp.class == Symbol
      if @sups[argsxp.to_s] 
        args = process exp.shift
	@depends.push DedRule.new(name.shift, argsxp, args)
        return s(name, argsxp, args)
      else
        args = process exp.shift
        if @nonmono[argsxp.to_s] 
          return s(name.shift, "neg")
        else
 	  return name
        end
      end
    else
      args = process argsxp
      return name
    end
  end

end

class DedRule
  # a small class to represent dependency info about 'rules' 
  attr_accessor :head, :body, :op
  def initialize(l, o, r)
    @head = cleanse(l) 
    @op = o

    @body = Array.new
    bod = r.shift
    if r.length == 1
      # negated atom
      @body.push [ bod.to_s , true]
    elsif r.length == 0 
      if bod.class == Symbol
        @body.push [ cleanse(bod.to_s), false ]
      else
        bod.each do |a|
          a = a.shift if a.class == Sexp
          @body.push [ cleanse(a.to_s), false ]
        end
      end
    end
  end
  def cleanse(str)
    ret = str.to_s.gsub("\"", "").gsub(":", "")
    return ret
  end
  def each
    @body.each do |b|
      yield [ @head, @op, b[0], b[1] ]
    end
  end
end

