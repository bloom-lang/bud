require 'rubygems'

require 'bud/sane_r2r'
require 'parse_tree'

class Bud


  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)

    depends = shred_rules

    strat = stratify(depends)

    smap = {}
    strat.tick
    strat.tick
    strat.tick
    strat.stratum.each do |s|
      #print "ST: STRAT OUT: #{s.inspect}\n"
      smap[s[0]] = s[1]
    end

    @rewritten_strata = []
    depends.sort{|a, b| oporder(a[1]) <=> oporder(b[1])}.each do |d|
      belongs_in = smap[d[0]]
      belongs_in = 0 if belongs_in.nil?
      if @rewritten_strata[belongs_in].nil?
        @rewritten_strata[belongs_in] = ""
      end
      @rewritten_strata[belongs_in] = @rewritten_strata[belongs_in] + "\n"+ d[3]
    end

    fout = File.new(self.class.to_s + "_rewritten.txt", "w")     
    @rewritten_strata.each_with_index do |r, i|
      fout.print "R[#{i}] :\n #{r}\n"
    end
    fout.close

    #visualize(strat, "#{self.class}_gvoutput")
    return @rewritten_strata
  end

  def shred_rules
    # to completely characterize the rules of a bud class we must extract
    # from all parent classes

    # after making this pass, we no longer care about the names of methods.
    # we are shredding down to the granularity of rule heads.
    depends = []
    done = {}
    curr_class = self.class
    until curr_class.nil?
      @declarations.each do |d|
        unless done[d]
          pt = ParseTree.translate(curr_class, d)
          unless pt[0].nil?
            rewriter = Rewriter.new
            rewriter.process(pt)
            rewriter.each {|re| depends << re}
            done[d] = true
          end
        end
      end
      curr_class = curr_class.superclass
    end
    return depends
  end



  def stratify(depends)
    strat = Stratification.new("localhost", 12345)
    #strat = StaticAnalysis.new("localhost", 12345)
    strat.tick

    @tables.each do |t|
      strat.tab_info << [t[0].to_s, t[1].class, t[1].schema.length]
    #  @table_meta << [t[0], t[1].class]
    end

    heads = bodies = {}
    depends.each do |d|
      op = d[2][0].to_s
      if op == "call" or op == "lasgn"
        bodies[d[0]] = true
      end
    end

    depends.each do |d|
      subparser = Extractor.new
      if bodies[d[0]]
        if !strat.tab_info.include? d[0]
          #strat.tab_info << [d[0], "temp alias", -1]
          @table_meta << [d[0], "temp alias"]
        end
      end
      #print "TRANSLATE: #{d[3]}\n"
      begin
        pt = ParseTree.translate(d[3])
      rescue
        print "Failed to translate #{d[3]}.\n"
        #raise RuntimeError($!)
      end
      if d[1] == '<'
        if d[3] =~ /-@/
          realop = "<-"
        else
          realop = "<+"
        end
      else
        realop = d[1]
      end
      subparser.process(pt)
      subparser.each do |k, v|
        strat.depends << [d[0], realop, k, v]
      end

      subparser.cols.each do |c|
        strat.col_alias << [d[0], c[0], c[1], c[2]]
      end
      subparser.each_alias do |a|
        strat.tab_alias << [d[0], a[0], a[1]]
      end
    end
    strat.tick
    return strat
  end

  def oporder(op)
    case op
      when '='
        return 0
      when '<<'
        return 1
      when '<='
        return 2
    else
      return 3
    end
  end

  
  def visualize(strat, name)
    self.tick
    @tables.each do |t|
      @table_meta << [t[0], t[1].class]
    end
    gv = Viz.new(strat.top_strat, strat.stratum, @table_meta, strat.cycle)
    gv.process(strat.depends)
    gv.finish(name)
  end



# ----indent.  fix.

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
      t = process exp.shift
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
    if op == "include?"
      # PAA: temporarily disabled to debug leak
      #@tabs[t] = 1
      ret = super
    elsif @nm[op]
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
    if @nmcontext
    end
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
    cxt = self.context[1].to_s
    # suppress those dang angle brackets
    if cxt == "arglist" or cxt == "masgn" 
      return "#{process_arglist(exp)}"
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
        if op[0,1].to_s != '<'
          raise "Invalid op (#{op[0,1].to_s}) in top-level block #{clause.inspect}\n"
        else
          shove(lhs, op, clause)
        end
      elsif lhs == ""
        #print "DO nothing; lhs is empty\n"
      else
        raise "Invalid top-level clause length #{len}: '#{clause.inspect}'"
      end
      
    end
    super exp
  end
end


end
