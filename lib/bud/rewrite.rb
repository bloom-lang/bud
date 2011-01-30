require 'rubygems'

require 'bud/sane_r2r'
require 'parse_tree'


class StateExtractor < SaneR2R
  attr_reader :tabs

  def initialize(context)
    @cxt = context
    @tabs = {}
    @ttype = nil
    super()
  end

  def process_zsuper(exp)
    # suppress superclass calls for this rewrite.
    # but consider obsoleting such calls via rewriting!
    exp.shift
    return ""
  end

  def process_block(exp)
    term  = exp.shift
    res = ""
    until term.nil?
      res += process(term) + "\n"
      term = exp.shift
    end
    return res
  end

  def process_defn(exp)
    if exp.first.to_s == "state"
      exp.shift
      ret = exp.shift
      process ret
    end
  end

  def process_vcall(exp)
    @ttype = exp.to_s
    super
  end

  def process_lit(exp)
    if exp.first.class == Symbol
      tab = exp.shift.to_s
      if @cxt.nil?
        res = tab
      else
        res =  @cxt.downcase + "_" + tab
      end
      @tabs[tab] = [res, @ttype]
      return ":" + res
    else
      super
    end
  end
end

class Rewriter < SaneR2R
  # the purpose of this class is to parse a bud class and provide
  # useful meta information about it for the purposes of analysis
  # and rewriting.  in particular, we want to populate a depends
  # relation and to decompose the user-supplied code by rule so
  # that we can rewrite it in stratum order.

  attr_reader :tabs, :cols, :aliases, :rule_indx

  def initialize(seed, tabcxt, prov)
    @rules = []
    @aliases = {}
    @suppress = 0
    @rule_indx = seed + 1
    @tabs = {}
    @currtab = nil
    @nm = {"group" => 1, "argagg" => 1, "include?" => 1}
    @nmcontext = 0
    @cols = []
    @provenance = prov
    @tabcxt = tabcxt
    newtab(nil)
    super()
  end

  #######################
  # iterators

  def each
    @rules.each do |rule|
      #(id, lhs, op, rhs, nm, block) = rule
      #yield [id, lhs, op, rhs, nm, block]
      yield rule
    end
  end

  # parser functions

  def process_array(exp)
    cxt = self.context[1].to_s
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
    #line_no = exp.line
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
      l = exp[0][0]
      if l.to_s == 'dvar'
        aliass = exp[0][1].to_s
        col = exp[1].to_s
        @cols << [aliass, col, @offset]
        @offset = @offset + 1
      elsif l.to_s == 'vcall'
        tab = resolve(exp[0][1])
      end
    else
      @lhs = true
    end
    if op == "include?" and !tab.nil?
      @tabs[tab] = 1
      ret = super
    elsif @nm[op]
      if op == "group"
        @grouping = true
      end
      @nmcontext += 1
      ret = super exp
      @nmcontext -= 1
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
    t = resolve(exp[0].to_s)
    if self.context[1].to_s == "call" or (self.context[1].to_s == "array")# and self.context[3].to_s == "lasgn")
      unless self.context[2].to_s == "block"
        newtab(t)
        @tabs[t] = @nmcontext
      end
    end
    # I want, for a given table, what its type is (eg interface or not, and if
    # so which type) and where "below me" it was defined.
    ret = resolve(exp.shift.to_s)
    @lhs = false
    return ret
  end

  def process_fcall(exp)
    # to keep our alias table clean
    t = exp[0].to_s
    newtab(t)
    super
  end

  # helper routines
  private

  def newtab(t)
    return if t == "print"
    @currtab = t
    @offset = 0
    @aliases[t] = []
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
    @rule_indx += 1
  end

  def extract_lhs(clause)
    # try to rethink this deep copy.
    # r2r heinously clobbers things it processes..
    l = Marshal.load(Marshal.dump(clause[1]))

    @lhs = true
    ret = (l.class == Symbol) ? l.to_s : l.nil? ? "" : process(l.clone)
    @lhs = false
    return ret
  end

  def resolve(res)
    if @tabcxt[res]
      last = @tabcxt[res][@tabcxt[res].length-1]
      if ((last[1] == "input" and @lhs) or (last[1] == "output" and !@lhs)) and not res =~ /_/ # and this is not already a fully-qualified table; pls fix
        tab = @tabcxt[res][(@tabcxt[res].length)-2][0]
      else
        tab = last[0]
      end
      return tab
    else
      return res
    end
  end
end
