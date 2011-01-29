require 'rubygems'

require 'bud/depanalysis'
require 'bud/provenance'
require 'bud/rewrite'
require 'bud/sane_r2r'
require 'parse_tree'


class Bud
  attr_reader :shredded_rules, :provides, :strat_state

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

    visualize(@strat_state, "#{self.class}_gvoutput", @shredded_rules) if @options['visualize']
    dump_rewrite if @options['dump']
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

  def each_relevant_ancestor
    on = false
    self.class.ancestors.reverse.each do |anc|
      if on
        yield anc
      elsif anc == Bud
        on = true
      end
    end
  end

  def rewrite(pt, tab_map, seed)
    unless pt[0].nil?
      rewriter = Rewriter.new(seed, tab_map, @options['provenance'])
      rewriter.process(pt)
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
        postamble = postamble + "#{last[0]} <= #{k}.map{|t| puts \"INPUT POSTAMBLE\" or t }\n\n"
      elsif last[1] == "output"
        postamble = postamble + "#{k} <= #{last[0]}.map{|t| puts \"OUTPUT POSTAMBLE\" or t }\n\n"
      else
        left = "#{k} <= #{last[0]}"
        right = "#{last[0]} <= #{k}"
        postamble = postamble + "#{left}.map{|t| puts \"VISIBILITy POSTAMBLE #{left} :: \" + t.inspect or t }\n\n"
        postamble = postamble + "#{right}.map{|t| puts \"VISIBILITy POSTAMBLE #{right} :: \" + t.inspect or t }\n\n"
      end
    end
    postamble = postamble + "\nend\n"   

    return rewrite(ParseTree.translate(postamble), {}, seed)
  end

  def shred_state(anc, tabs)
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
    rules = []
    seed = 0
    rulebag = {}
    tabs = {} 
    each_relevant_ancestor do |anc|
      tabs = shred_state(anc, tabs) if @options['scoping']
      @declarations.each do |meth_name|
        rw = rewrite(ParseTree.translate(anc, meth_name), tabs, seed)
        unless rw.nil? 
          seed = rw.rule_indx
          rulebag[meth_name] = rw
        end
      end
    end
    rulebag.each_pair do |k, v|
      v.each do |val|
        #puts "RULEBAG #{k.inspect} = #{val.inspect}"
        rules << val
      end
    end
    if @options['scoping']
      res = write_postamble(tabs, seed + 100)
      rules.concat(res)
    end
    return rules
  end

  def stratify(depends)
    strat = Stratification.new
    strat.tick

    @tables.each do |t|
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
      # seriously, consider named notation for d.
      strat.depends << [ d[0], d[1], realop, d[3], d[4] ]
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

  def do_cards
    return unless options['visualize']
    cards = {}
    @tables.each do |t|
      puts "#{@budtime}, #{t[0]}, #{t[1].length}"
      cards[t[0].to_s] = t[1].length
    end
    write_svgs(cards)
    write_html
    
  end

  def write_svgs(c)
    `mkdir time_pics`
    return if @strat_state.nil?
    puts "construct viz with cards = #{c.class}"
    gv = Viz.new(@strat_state.stratum, @tables, @strat_state.cycle, nil, c)
    gv.process(@strat_state.depends)
    gv.finish("time_pics/#{self.class}_tm_#{@budtime}")
  end

  def write_html
    nm = "#{self.class}_tm_#{@budtime}"
    prev = "#{self.class}_tm_#{@budtime-1}"
    nxt = "#{self.class}_tm_#{@budtime+1}"
    fout = File.new("time_pics/#{nm}.html", "w")
    fout.puts "<center><h1>#{self.class} @ #{@budtime}</h1><center>"
    #fout.puts "<img src=\"#{ENV['PWD']}/time_pics/#{nm}.svg\">"
    fout.puts "<embed src=\"#{ENV['PWD']}/time_pics/#{nm}.svg\" width=\"100%\" height=\"75%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />"
    #fout.puts "<embed src=\"#{ENV['PWD']}/time_pics/#{nm}.svg\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />"
    fout.puts "<hr><h2><a href=\"#{ENV['PWD']}/time_pics/#{prev}.html\">last</a>"
    fout.puts "<a href=\"#{ENV['PWD']}/time_pics/#{nxt}.html\">next</a>"
    fout.close
  end

  def visualize(strat, name, rules, depa=nil)
    #@tables.each do |t|
    #  @table_meta << [t[0], t[1].class]
    #end
    #gv = Viz.new(strat.stratum, @table_meta, strat.cycle, depa)
    puts "VIZZ"
    gv = Viz.new(strat.stratum, @tables, strat.cycle, depa)
    gv.process(strat.depends)
    gv.dump(rules)
    gv.finish(name)
  end
end
