require 'bud/provenance'
require 'bud/rewrite'
require 'bud/graphs'
require 'bud/state'
require 'parse_tree'

class BudMeta
  include BudState
  attr_reader :depanalysis, :decls

  def initialize(bud_instance, declarations)
    @bud_instance = bud_instance
    @declarations = declarations
    @decls = []
  end

  def meta_rewrite
    # N.B. -- parse_tree will not be supported in ruby 1.9.
    # however, we can still pass the "string" code of bud modules
    # to ruby_parse (but not the "live" class)
    shred_rules
    top_stratum = stratify
    stratum_map = binaryrel2map(@bud_instance.t_stratum)

    done = {}
    rewritten_strata = Array.new(top_stratum + 1, "")
    @bud_instance.t_rules.sort{|a, b| oporder(a.op) <=> oporder(b.op)}.each do |d|
      unless done[d.rule_id]
        # joins may have to be restated
        belongs_in = stratum_map[d.lhs]
        belongs_in ||= 0
        if d.op == "="
          (belongs_in..top_stratum).each do |i|
            rewritten_strata[i] += "\n" + d.src
          end
        else
          rewritten_strata[belongs_in] += "\n" + d.src
        end
        done[d.rule_id] = true
      end
    end

    @depanalysis = DepAnalysis.new
    @bud_instance.t_depends_tc.each {|d| @depanalysis.depends_tc << d}
    @bud_instance.t_provides.each {|p| @depanalysis.providing << p}
    3.times { @depanalysis.tick }

    @depanalysis.underspecified.each do |u|
      puts "Warning: underspecified dataflow: #{u.inspect}"
    end
    dump_rewrite(rewritten_strata) if @bud_instance.options[:dump_rewrite]

    return rewritten_strata
  end

  def binaryrel2map(rel)
    map = {}
    rel.each do |s|
      raise Bud::BudError unless s.length == 2
      map[s[0]] = s[1]
    end
    return map
  end

  def rewrite(parse_tree, seed)
    unless parse_tree[0].nil?
      u = Unifier.new
      pt = u.process(parse_tree)
      rewriter = RW.new(seed)
      rewriter.process(pt)
      #rewriter.rules.each {|r| puts "RW: #{r.inspect}"}
      return rewriter
    end
  end

  def shred_state(anc)
    return unless @bud_instance.options[:scoping]
    stp = ParseTree.translate(anc, "__#{@bud_instance.class}__state")
    return if stp[0].nil?
    state_reader = StateExtractor.new(anc.to_s)
    u = Unifier.new
    pt = u.process(stp)
    res = state_reader.process(pt)
    @decls += state_reader.decls
  end

  def shred_rules
    # to completely characterize the rules of a bud class we must extract
    # from all parent classes/modules
    # after making this pass, we no longer care about the names of methods.
    # we are shredding down to the granularity of rule heads.
    seed = 0
    rulebag = {}
    each_relevant_ancestor do |anc|
      shred_state(anc)
      @declarations.each do |meth_name|
        rw = rewrite(ParseTree.translate(anc, meth_name), seed)
        if rw
          seed = rw.rule_indx
          rulebag[meth_name] = rw
        end
      end
    end

    rulebag.each_value do |v|
      v.rules.each do |r|
        check_rule(r)
        @bud_instance.t_rules << r
      end
      v.depends.each do |d|
        @bud_instance.t_depends << d
      end
    end
  end

  # Quick sanity-check on rules
  # Rule format: [rule_id, lhs, op, rule_txt]
  def check_rule(r)
    return if @bud_instance.options[:disable_sanity_check]

    legal_ops = ["<<", "<+", "<-", "<~", "<=", "="]
    lhs = r[1]
    op = r[2]
    unless legal_ops.include? op
      raise Bud::CompileError, "Illegal operator '#{op}'"
    end

    # Allow new variables but only on the LHS of an equality operator ("=")
    unless (@bud_instance.tables.has_key? lhs.to_sym or op == "=")
      raise Bud::CompileError, "Unrecognized rule LHS '#{lhs}'"
    end
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

  def stratify
    strat = Stratification.new
    @bud_instance.tables.each do |t|
      strat.tab_info << [t[0].to_s, t[1].class, t[1].schema.length]
    end
    @bud_instance.t_depends.each do |d|
      strat.depends << d
    end
    strat.tick

    # Copy computed data back into Bud runtime
    strat.stratum.each {|s| @bud_instance.t_stratum << s}
    strat.depends_tc.each {|d| @bud_instance.t_depends_tc << d}
    strat.cycle.each {|c| @bud_instance.t_cycle << c}
    if strat.top_strat.length > 0
      top = strat.top_strat.first.stratum
    else
      top = 1
    end
    return top
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

  def dump_rewrite(strata)
    fout = File.new("#{@bud_instance.class}_rewritten.txt", "w")
    fout.puts "Declarations:"
    @decls.each do |d|
      fout.puts d
    end

    strata.each_with_index do |r, i|
      fout.puts "R[#{i}]:\n #{r}"
    end
    fout.close
  end
end
