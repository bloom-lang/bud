require 'rubygems'

require 'bud/sane_r2r'
require 'bud/rewrite'
require 'bud/provenance'
require 'parse_tree'

class Bud

  attr_reader :shredded_rules

  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)

    @shredded_rules = shred_rules
    strat = stratify(@shredded_rules)

    smap = {}
    strat.stratum.each do |s|
      #print "ST: STRAT OUT: #{s.inspect}\n"
      smap[s[0]] = s[1]
    end

    # temporary suppression of delta rule duplication
    done = {}
    @rewritten_strata = []
    @shredded_rules.sort{|a, b| oporder(a[2]) <=> oporder(b[2])}.each do |d|
      belongs_in = smap[d[1]]
      belongs_in = 0 if belongs_in.nil?
      if @rewritten_strata[belongs_in].nil?
        @rewritten_strata[belongs_in] = ""
      end
  
      unless done[d[0]]
        @rewritten_strata[belongs_in] = @rewritten_strata[belongs_in] + "\n"+ d[5] 
      end
      done[d[0]] = true
    end

    ###@rewritten_strata << write_postamble
    ###create_delta_tables
    visualize(strat, "#{self.class}_gvoutput") if @options['visualize']
    dump_rewrite if @options['dump']
    return @rewritten_strata
  end

  def write_postamble
    postamble = ''
    @tables.each do |t|
      nm = t[1].name
      if nm.class == Symbol
        postamble = postamble + "#{nm.to_s} <+ #{nm.to_s}_delta.map{|i| i }\n"
      end
    end
    return postamble
  end

  def dump_rewrite
    fout = File.new(self.class.to_s + "_rewritten.txt", "w")     
    @rewritten_strata.each_with_index do |r, i|
      fout.print "R[#{i}] :\n #{r}\n"
    end
    fout.close
  end

  def tables_each
    @tables.each do |t|
      #print "TABE: #{t.inspect}\n"
      if t[1].name.class == Symbol
        yield t[1]
      end
    end
  end

  def create_delta_tables
    statements = []
    tables_each do |t|
      tm = t.name
      dname = tm.to_s + "_delta"
      str = "scratch :" + dname + ", [" + t.schema.map{|i| "\"#{i}\""}.join(",") + "]"
      statements << str
    end
    statements.each do |st|
      eval ( st )
    end
  end

  def provenance_extend
    #print "EXTEND!\n"
    tables_each do |t|
      #print "pUSH ontto #{t.name.to_s} schema #{t.schema.class} (#{t.schema.join(",")})\n"
      t.schema.push("prov")
      # con cari~no
      t.keys.push("prov")
      t.schema_accessors
      #print "now it's #{t.schema.join(",")}\n"
    end
    #print "DONE EXtending\n"
  end

  def shred_rules
    # to completely characterize the rules of a bud class we must extract
    # from all parent classes

    # after making this pass, we no longer care about the names of methods.
    # we are shredding down to the granularity of rule heads.
    depends = []
    subd = {}
    done = {}
    curr_class = self.class
    seed = 0
    until curr_class.nil?
      @declarations.each do |d|
        unless done[d]
          pt = ParseTree.translate(curr_class, d)
          unless pt[0].nil?
            rewriter = Rewriter.new(seed, @options['provenance'])
            rewriter.process(pt)
            rewriter.each {|re| depends << re}
            done[d] = true
            seed = rewriter.rule_indx
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
    end

    depends.each do |d|
      #print "INSIDE DEP: #{d.inspect}\n"
      if d[2] == '<'
        if d[5] =~ /-@/
          realop = "<-"
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

  
  def visualize(strat, name)
    #self.tick
    @tables.each do |t|
      @table_meta << [t[0], t[1].class]
    end
    gv = Viz.new(strat.top_strat, strat.stratum, @table_meta, strat.cycle)
    gv.process(strat.depends)
    gv.finish(name)
  end
end
