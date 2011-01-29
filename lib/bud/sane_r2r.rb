# Ruby2Ruby is a fine idea, but it crashes on a lot of perfectly
# legal ruby.  SaneR2R extends Ruby2Ruby so that it can handle Bud
# syntax, either by creating handlers for expression types (e.g. 
# masgn, dasgn_curr, vcall) not declared in r2r, or overriding 
# handlers (e.g. call, defn) for special case output.

require 'parse_tree'
require 'ruby2ruby'
require 'parse_tree_extensions'

class SaneR2R < Ruby2Ruby
  def process_zarray(exp)
    # band-aid: ruby2ruby doesn't know what a zarray is,
    # and neither do I.  for now returning its contents as a string
    # works for all existing examples...
    return '[' + exp.to_s + ']'
  end

  def process_defn(exp)
    type = exp[1].first
    if type.to_s == "bmethod" 
      l = exp.shift
      r = exp.shift
      return "#{l} #{r}"
    elsif type.to_s == "ivar"
      exp.shift
      exp.shift
      return "IVAR: #{process exp.shift}"
    else
      super
    end
  end

  def process_masgn(exp)
    if exp[1].nil? 
      return process_fcall_list(exp)
    else
      super
    end
  end

  def process_call(exp)
    name = exp[1].to_s
    if exp.length == 2
      return "#{process exp.shift}.#{exp.shift.to_s} "
    else
      return super(exp)
    end
  end

  def process_dvar(exp)
    return exp.shift.to_s
  end

  def process_dasgn_curr(exp)
    # for now...
    l = exp.shift
    r = process exp.shift
    if r.nil? 
      return "#{l} "
    else 
      return "#{l} = #{r} "
    end
  end

  def process_vcall(exp)
    foo = exp.shift
    return foo.to_s
  end

  def process_fcall_list(exp)
    str = ""
    until exp.empty?
      l = exp.shift
      if l.nil? 
        # nothing
      elsif l.class == Symbol
        str += l.to_s
      else
        str += process(l)
      end
    end
    return "#{str} "
  end

  def process_fcall(exp)
    if exp.length == 2
      l = exp.shift
      r = exp.shift
      r.shift
      return l.to_s + "(" + process_arglist(r) + ")"
    else
      return process_fcall_list(exp)
    end
  end
end
