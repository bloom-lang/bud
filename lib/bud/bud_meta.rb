require 'rubygems'

require 'bud/depanalysis'
require 'bud/provenance'
require 'bud/rewrite'
require 'bud/sane_r2r'
require 'bud/graphs'
require 'parse_tree'


class BudMeta
  attr_reader :shredded_rules, :provides, :strat_state, :depanalysis

  def initialize(bud_instance, declarations)
    # need: provides, options, declarations, class, tables, self for viz
    # meanwhile, viz needs:  class, object_id, options, tables, budtime
    # meh.
    @bud_instance = bud_instance
    @declarations = declarations
  end

  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)
    @defns = []
    @shredded_rules = shred_rules

    @strat_state = stratify(@shredded_rules)
    smap = binaryrel2map(@strat_state.stratum)

    done = {}
    @rewritten_strata = []
    @shredded_rules.sort{|a, b| oporder(a[2]) <=> oporder(b[2])}.each do |d|
      belongs_in = smap[d[1]]
      belongs_in = 0 if belongs_in.nil?

      @rewritten_strata[belongs_in] ||= ""
      unless done[d[0]]
        @rewritten_strata[belongs_in] += "\n" + d[5]
      end
      done[d[0]] = true
    end

    @depanalysis = DepAnalysis.new
    @strat_state.depends_tc.each{|d| @depanalysis.depends_tc << d }
    @bud_instance.provides.each{|p| @depanalysis.providing << p}
    3.times { @depanalysis.tick }
    @depanalysis.underspecified.each{|u| puts "UNDERSPECIFIED: #{u.inspect}"}
    
    ##visualize(@strat_state, "#{self.class}_gvoutput", @shredded_rules, @depanalysis) if @bud_instance.options[:visualize]
    dump_rewrite if @bud_instance.options[:dump]
    return @rewritten_strata
  end

  def binaryrel2map(rel)
    smap = {}
    rel.each do |s|
      smap[s[0]] = s[1]
    end
    return smap
  end

  def dump_rewrite
    fout = File.new(self.class.to_s + "_rewritten.txt", "w")
    fout.puts "Declarations:"
    @defns.each do |d|
      fout.puts d
    end
    
    @rewritten_strata.each_with_index do |r, i|
      fout.puts "R[#{i}] :\n #{r}"
    end
    fout.close
  end

  def rewrite(parse_tree, tab_map, seed)
    unless parse_tree[0].nil?
      rewriter = Rewriter.new(seed, tab_map, @bud_instance.options[:provenance])
      rewriter.process(parse_tree)
    end
    return rewriter
  end

  def write_postamble(tabs, seed)
    # rationale for the postamble: 
    # for any module M, any table T declared within is internally named m_t.
    # if T is an input interface, we need to add a rule m_t <- t.
    # if T is an output interface, we need a rule t <- m_t.

    postamble = "def foobar\n"
    tabs.each_pair do |k, v|
      last = v.last
      if last[1] == "input"  
        postamble += "#{last[0]} <= #{k}.map{|t| puts \"INPUT POSTAMBLE\" or t }\n\n"
      elsif last[1] == "output"
        postamble += "#{k} <= #{last[0]}.map{|t| puts \"OUTPUT POSTAMBLE\" or t }\n\n"
      else
        left = "#{k} <= #{last[0]}"
        right = "#{last[0]} <= #{k}"
        postamble += "#{left}.map{|t| puts \"VISIBILITy POSTAMBLE #{left} :: \" + t.inspect or t }\n\n"
        postamble += "#{right}.map{|t| puts \"VISIBILITy POSTAMBLE #{right} :: \" + t.inspect or t }\n\n"
      end
    end
    postamble += "\nend\n"   

    return rewrite(ParseTree.translate(postamble), {}, seed)
  end

  def shred_state(anc, tabs)
    return {} unless @bud_instance.options[:scoping]
    stp = ParseTree.translate(anc, "state")
    return tabs if stp[0].nil?
    state_reader = StateExtractor.new(anc.to_s)
    res = state_reader.process(stp)
    # create the state
    #puts "DEFN : #{res}"
    @defns << res
    eval(res)
    state_reader.tabs.each_pair do |k, v| 
      #puts "tab KEYPAIR #{k.inspect} = #{v.inspect}"
      tabs[k] ||= []
      tabs[k] << v 
    end
    return tabs
  end

  def shred_rules
    # to completely characterize the rules of a bud class we must extract
    # from all parent classes/modules
    # after making this pass, we no longer care about the names of methods.
    # we are shredding down to the granularity of rule heads.
    seed = 0
    rulebag = {}
    each_relevant_ancestor do |anc|
      tabs = shred_state(anc, tabs)
      @declarations.each do |meth_name|
        rw = rewrite(ParseTree.translate(anc, meth_name), tabs, seed)
        unless rw.nil? 
          seed = rw.rule_indx
          rulebag[meth_name] = rw
        end
      end
    end

    rules = []
    rulebag.each_pair do |k, v|
      v.each do |val|
        #puts "RULEBAG #{k.inspect} = #{val.inspect}"
        rules << val
      end
    end
    if @bud_instance.options[:scoping]
      res = write_postamble(tabs, seed + 100)
      rules.concat(res)
    end
    return rules
  end

  def each_relevant_ancestor
    on = false
    @bud_instance.class.ancestors.reverse.each do |anc|
      if on
        yield anc
      elsif anc == Bud
        on = true
      end
    end
  end

  def stratify(depends)
    strat = Stratification.new
    strat.tick

    @bud_instance.tables.each do |t|
      strat.tab_info << [t[0].to_s, t[1].class, t[1].schema.length]
    end

    depends.each do |d|
      if d[2] == '<'
        if d[5] =~ /-@/
          realop = "<-"
        elsif d[5] =~ /\~ \)/
          # hackerly
          realop = "<~"
        else  
          realop = "<+"
        end
      else
        realop = d[2]
      end
      # Format: rule, head, op, body, neg
      # XXX: consider named notation for d.
      strat.depends << [d[0], d[1], realop, d[3], d[4]]
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
end
