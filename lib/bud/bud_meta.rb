require 'rubygems'

require 'bud/sane_r2r'
require 'parse_tree'

class Extractor < SaneR2R
  attr_reader :tabs, :cols, :aliases

  def initialize
    @tabs = {}
    @currtab = nil
    @nm = {"group", 1, "argagg", 1, "include?", 1}
    @nmcontext = 0
    @aliases={}
    @cols = []
    newtab(nil)
    super
  end

  def newtab(t)
    @currtab = t
    @offset = 0
    @aliases[t] = []
  end

  def process_dasgn_curr(exp)
    @aliases[@currtab] << exp[0].to_s
    super
  end

  def process_call(exp)
    op = exp[1].to_s
    if exp.length == 3 or exp.length == 4
      exp.shift
      exp.shift
    elsif exp.length == 2
      l = exp[0][0] 
      if l.to_s == 'dvar'
        aliass = exp[0][1].to_s
        col = exp[1].to_s
        @cols << [aliass, col, @offset]
        @offset = @offset + 1
      elsif l.to_s == 'vcall'
        tab = exp[0][1]
      end
    end
    if @nm[op]
      @nmcontext = @nmcontext + 1
      ret = super exp
      @nmcontext = @nmcontext - 1
    else
      ret = super
    end
    return ret
  end

  def process_vcall(exp)
    t = exp[0].to_s
    newtab(t)
    @tabs[t] = @nmcontext
    super
  end

  def each
    @tabs.each_pair do |k,v|
      yield [k, v]
    end
  end
  def each_alias
    @aliases.each_pair do |k, v|
      unless v.empty? 
        v.each do |i|
          yield [k, i]
        end
      end
    end
  end
  def each_join
    @aliases.each_pair do |k, v|
      if v.empty? 
        yield k
      end
    end
  end
end

class Rewriter < SaneR2R
  # the purpose of this class is to parse a bud class and provide
  # useful meta information about it for the purposes of analysis 
  # and rewriting.  in particular, we want to populate a depends
  # relation and to decompose the user-supplied code by rule so 
  # that we can rewrite in in stratum order.

  def initialize
    @rules = {}
    @tabs = []
    @suppress = 0; 
    super
  end

  #def process(exp)
  #  print "PROCESS(): #{exp.inspect}\n"
  #  super
  #end

  def process_call(exp)
    if exp.length == 2
      print "\tCASE 2\n"
      super
    elsif exp.length == 3
      print "\tCASE 3\n"
      @suppress = @suppress + 1
      ret = super
      #@suppress = @suppress - 1 
      return ret
    else
      #print "\tCASE other\n"
      super
    end
  
  end

  def each_tab(key)
    @rules[key].each do |r|
      yield r.unshift(key)
    end
  end
  
  def each
    @rules.each_key {|k| each_tab(k) {|e| yield e} }
  end

  def empty(exp)
    until exp.empty?
      exp.shift
    end
    return ""
  end

  def process_array(exp)
    if (@suppress > 0) #or (exp.length < 2)
      @suppress = @suppress - 1 if @suppress > 0
      return "#{process_arglist(exp)}"
    else
      return "[#{process_arglist(exp)}]"
    end
  end
  
  def process_fcall(exp)
    print "process FCALL: #{exp.inspect}\n"
    super
  end
  
  def process_masgn(exp)
    @suppress = @suppress + 1
    ret = super
    @suppress = @suppress - 1
    return ret
  end

  def process_defn(exp)
    fst = exp[0]
    if fst.to_s != 'state' and fst.to_s != 'initialize'
      super exp
    end
    empty(exp)
  end

  def shove(lhs, op, whole)
    if !@rules[lhs]
      @rules[lhs] = []
    end
    copy = whole.clone
    
    @rules[lhs] << [op, copy, process(whole)]
  end

  def process_block(exp)
    # shift off the 'args'
    exp.shift
    until exp.empty?
      clause = exp.shift
      len = clause.length
      l = Marshal.load(Marshal.dump(clause[1]))
      lhs = (l.class == Symbol) ? l.to_s : l.nil? ? "" : process(l.clone)
      if len == 3
        # 'assignment'
        shove(lhs, "=", clause)
      elsif len == 4
        # 'rule'
        op = clause[2].to_s
        shove(lhs, op, clause)
      elsif lhs == ""
        print "DO nothing\n"
      else
        print "coming out of #{exp.inspect}\n"
        raise "Invalid top-level clause length #{len}: '#{clause.inspect}'"
      end
      
    end
    super exp
    
  end

end



