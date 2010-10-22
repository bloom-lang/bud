require 'rubygems'

require 'bud/sane_r2r'
require 'parse_tree'


class Rewriter < SaneR2R
  # the purpose of this class is to parse a bud class and provide
  # useful meta information about it for the purposes of analysis 
  # and rewriting.  in particular, we want to populate a depends
  # relation and to decompose the user-supplied code by rule so 
  # that we can rewrite in in stratum order.

  attr_reader :tabs, :cols, :aliases, :rule_indx

  def initialize(seed, prov)
    @rules = []
    @aliases={}
    @suppress = 0; 
    @rule_indx = seed + 1
    @tabs = {}
    @currtab = nil
    @nm = {"group", 1, "argagg", 1, "include?", 1}
    @nmcontext = 0
    @cols = []
    @provenance = prov
    newtab(nil)
    super()
  end

  # helper routines
  def newtab(t)
    return if t == "print"
    @currtab = t 
    @offset = 0
    @aliases[t] = []
  end

  def delta_munge(str, dtab, sub)
    return eval("str.#{sub}(\"TABLE(#{dtab})\", dtab + \"_delta\")")
  end

  def remove_annotations(str)
    # PAA: surely there is a better way.
    while str =~ /TABLE\(([^)]+)\)/ 
      $m = $1
      str = str.sub(/TABLE\([^)]+?\)/, $m)
    end
    return str
  end

  def empty(exp)
    until exp.empty?
      exp.shift
    end
    return ""
  end

  def shove(lhs, op, whole)
    result = process(whole)
    if @tabs.keys.length == 0
      @rules << [@rule_indx, lhs, op, nil, nil, result]
    else
      @tabs.each_pair do |k, v|
        @rules << [@rule_indx, lhs, op, k, v, result]
      end
    end
    @tabs = {}
    @rule_indx = @rule_indx + 1
  end

  def extract_lhs(clause)
    # try to rethink this deep copy.  
    # r2r heinously clobbers things it processes..
    l = Marshal.load(Marshal.dump(clause[1]))
    return  (l.class == Symbol) ? l.to_s : l.nil? ? "" : process(l.clone)
  end

  #######################
  # iterators

  def each_deltas
    @rules.each do |rule| 
      #print "RULE: #{rule.inspect}\n"
      (id, lhs, op, rhs, nm, block) = rule
      # nasty
      dlhs = remove_annotations(lhs) + "_delta"
      drhs = remove_annotations(rhs) + "_delta"

      clean_lhs = delta_munge(block, remove_annotations(lhs), "sub")
      newblock = delta_munge(clean_lhs, remove_annotations(rhs), "gsub")
      final_block = remove_annotations(newblock)
      yield [id, dlhs, op, drhs, nm, final_block]
    end
  end

  def each
    done = {}
    @rules.each do |rule|
      #print "RULE: #{rule.inspect}\n"
      (id, lhs, op, rhs, nm, block) = rule
      clhs = remove_annotations(lhs)
      crhs = remove_annotations(rhs)
      body = remove_annotations(block)
      yield [id, clhs, op, crhs, nm, body]  
    end
  end

  # parser functions

  def process_array(exp)
    cxt = self.context[1].to_s
    #print "CXT #{cxt} grouoing #{@grouping} exp #{exp.inspect}\n"
    # suppress those dang angle brackets...
    if cxt == "masgn" 
      return "#{process_arglist(exp)}"
    elsif cxt == "arglist"
      if @grouping and @provenance
        return "#{process_arglist(exp)}, prov_agg(#{@currtab}.prov)"
      else
        return "#{process_arglist(exp)}"
      end
    elsif cxt == 'fcall' or cxt == 'array'
      return "[#{process_arglist(exp)}]"
    elsif @provenance
      quotedlist = @aliases[@currtab].map{|a| "\"#{a}\", #{a}"}.join(",")
      return "[#{process_arglist(exp)},  prov_cat(#{@rule_indx}, #{quotedlist})]"
    else
      return "[#{process_arglist(exp)}]"
    end
  end
    
  def process_defn(exp)
    fst = exp[0]
    if fst.to_s != 'state' and fst.to_s != 'initialize'
      super exp
    end
    empty(exp)
  end

  def process_block(exp)
    # shift off the 'args'
    #print "BLOCK: #{exp.inspect}\n"
    exp.shift
    until exp.empty?
      clause = exp.shift
      lhs = extract_lhs(clause)
      len = clause.length
      if len == 3
        # 'assignment'
        shove(lhs, "=", clause)
      elsif len == 4
        # 'rule'
        op = clause[2].to_s
        if op[0,1].to_s != '<'
          raise "Invalid op (#{op[0,1].to_s}) in top-level block #{clause.inspect}\n"
        else
          shove(lhs, op, clause)
        end
      else
        raise "Invalid top-level clause length #{len}: '#{clause.inspect}'"
      end
    end
    super exp
  end

  def process_dasgn_curr(exp)
    @aliases[@currtab] << exp[0].to_s unless exp[0].to_s.nil?
    super
  end

  def process_call(exp)
    op = exp[1].to_s
    if exp.length == 2
      #print "CALL HAS 2: #{exp.inspect}\n"
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
    if op == "include?" and !tab.nil?
      @tabs[tab] = 1
      ret = super
    elsif @nm[op]
      if op == "group"
        #print "GRoUPING(#{exp.length}): #{exp.inspect}. need to add a provenance argument\n"
        @grouping = true
      end
      @nmcontext = @nmcontext + 1
      ret = super exp
      @nmcontext = @nmcontext - 1
    else
      ret = super
    end
    @grouping = false
    return ret
  end

  def process_lvar(exp)
    newtab(exp[0].to_s)
    # special case: joins are never nonmonotonic
    @tabs[exp[0].to_s] = 0
    super
  end

  def process_vcall(exp)
    t = exp[0].to_s
    if self.context[1].to_s == "call" or (self.context[1].to_s == "array" and self.context[3].to_s == "lasgn")
      unless self.context[2].to_s == "block"
        newtab(t)
        @tabs[t] = @nmcontext
      end
    end
    return "TABLE(#{exp.shift.to_s})"
  end

  def process_fcall(exp)
    # to keep our alias table clean
    t = exp[0].to_s
    newtab(t)
    super
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



