require 'rubygems'
require 'bud'

module Validate
  state do
    scratch :dep, [:body, :head, :label]
    scratch :dep_tc, [:body, :head, :members]
    scratch :scc, [:pred, :cluster]
    scratch :scc_raw, scc.schema
    scratch :new_dep, [:body, :head, :label]
    scratch :labeled_path, [:body, :head, :path, :label]
    scratch :full_path, labeled_path.schema
    scratch :ndn, new_dep.schema
    scratch :iinterface, t_provides.schema
    scratch :ointerface, t_provides.schema
    scratch :iin, t_provides.schema
    scratch :iout, t_provides.schema
  end

  bloom do
    dep <= t_depends do |d|
      [d.body, d.lhs, labelof(d.op, d.nm)]
    end

    dep_tc <= dep do |d|
      [d.body, d.head, Set.new([d.body, d.head])]
    end
    dep_tc <= (dep * dep_tc).pairs(:head => :body) do |d, t|
      [d.body, t.head, t.members | [d.head]]
    end

    scc_raw <= dep_tc do |d|
      if d.head == d.body
        [d.head, d.members.to_a.sort]
      end
    end

    scc <= scc_raw.reduce(Hash.new) do |memo, i|
      memo[i.pred] ||= []
      memo[i.pred] |= i.cluster
      memo
    end

    new_dep <= (dep * scc * scc).combos do |d, s1, s2|
      if d.head == s1.pred and d.body == s2.pred
        ["#{s2.cluster.join(",")}_IN", "#{s1.cluster.join(",")}_OUT", d.label]
      end
    end
    new_dep <= (dep * scc).pairs(:body => :pred) do |d, s|
      ["#{s.cluster.join(",")}_OUT", d.head, d.label] unless s.cluster.include? d.head
    end
    new_dep <= (dep * scc).pairs(:head => :pred) do |d, s|
      [d.body, "#{s.cluster.join(",")}_IN", d.label] unless s.cluster.include? d.body
    end

    ndn <= dep.notin(scc, :body => :pred)
    new_dep <= ndn.notin(scc, :head => :pred)
  end

  bloom :channel_inputs do
    temp :dummy_input <= t_provides do |p|
      if p.input and coll_type(p.interface) == Bud::BudChannel
        [p.interface]
      end
    end
    dep <= dummy_input{|i| ["#{i.first}_INPUT", i.first, "A"]}
    dep <= dummy_input{|i| ["#{i.first}_INPUT", i.first, "A"]}
    t_provides <= dummy_input{|i| ["#{i.first}_INPUT", true]}
  end

  bloom :full_paths do
    iin <= t_provides{|p| p if p.input}
    iout <= t_provides{|p| p if !p.input}
    iinterface <= iin.notin(new_dep, :interface => :head)
    ointerface <= iout.notin(new_dep, :interface => :body)

    labeled_path <= (new_dep * iinterface).pairs(:body => :interface) do |d, p|
      [d.body, d.head, [d.body, d.head], [d.label]]
    end
    labeled_path <= (labeled_path * new_dep).pairs(:head => :body) do |p, d|
      [p.body, d.head, p.path + [d.head], p.label + [d.label]]
    end

    full_path <= (labeled_path * ointerface).lefts(:head => :interface)
  end

  def validate
    dp = Set.new
    divergent_preds.each do |p| 
      dp.add(p.coll)
    end
    report = []
    full_path.to_a.each do |p|
      state = ["Bot"]
      start_a = -1
      p.label.each_with_index do |lbl, i|
        if lbl == "A"
          start_a = i + 1
        end
        os = state.first
        state = do_collapse(state, [lbl])
      end
      if dp.include? p.head
        report << (p.to_a + [:unguarded, ["D"]])
      else
        report << (p.to_a + [:path, state])
      end
    end
    return report
  end

  def do_collapse(left, right)
    l = left.pop
    r = right.shift
    left + collapse(l, r) + right
  end

  def labelof(op, nm)
    if op == "<~"
      "A"
    elsif nm
      "N"
    else
      "Bot"
    end
  end

  def collapse(left, right)
    return [right] if left == 'Bot'
    return [left] if right == 'Bot'
    return [left] if left == right
    return ['D'] if left == 'D' or right == 'D'
    # CALM
    return ['D'] if left == 'A' and right =~ /N/
    # sometimes we cannot reduce
    return [left, right]
  end
end


module GuardedAsync
  include Validate
  state do
    scratch :meet, [:chan, :partner, :at, :lpath, :rpath]
    scratch :meet_stg, meet.schema
    scratch :channel_race, [:chan, :partner, :to, :guarded]
    scratch :dep_tc_type, [:body, :head, :types]
    scratch :divergent_preds, [:coll]
  end

  bloom do
    dep_tc_type <= dep do |d|
      btab = coll_type(d.body)
      htab = coll_type(d.head)
      [d.body, d.head, Set.new([btab, htab])]
    end
    dep_tc_type <= (dep * dep_tc_type).pairs(:head => :body) do |d, t|
      htab = coll_type(d.head)
      [d.body, t.head, t.types | [htab]]
    end

    meet_stg <= (dep_tc_type * dep_tc_type).pairs(:head => :head) do |l, r|
      ltab = self.tables[l.body.to_sym]
      rtab = self.tables[r.body.to_sym]
      if ltab.class == Bud::BudChannel and rtab.class == Bud::BudChannel and l.body != r.body
        [l.body, r.body, l.head, l.types, r.types]
      end
    end

    meet <= meet_stg.notin(dep_tc_type, :chan => :body, :partner => :head)
    channel_race <= meet{|m| [m.chan, m.partner, m.at, guarded(m.lpath, m.rpath)]}
    divergent_preds <= channel_race{|r| [r.to] unless r.guarded}
    divergent_preds <= (channel_race * dep_tc_type).pairs(:to => :body){|r, t| [t.head] unless r.guarded}
  end

  def coll_type(nm)
    tab = self.tables[nm.to_sym]
    if tab.nil?
      tab = self.lattices[nm.to_sym]
    end
    tab.class
  end

  def guarded(lpath, rpath)
    if lpath.include? Bud::BudTable or lpath.include? Bud::LatticeWrapper
      if rpath.include? Bud::BudTable or rpath.include? Bud::LatticeWrapper
        return true
      end
    end
    false
  end
end

require 'bud/labeling/bloomgraph'
require 'bud/labeling/budplot_style'

module MetaMods
  include Validate
  include GuardedAsync
  include BloomGraph
  include PDG
end

class Label
  attr_reader :f

  def initialize(mod)
    @report = nil
    @mod = Object.const_get(mod)
    if @mod.class == Class
      nc = new_class_from_class(@mod)
    elsif @mod.class == Module
      nc = new_class(@mod)
    else
      raise "#{mod} neither class nor module"
    end
    @f = nc.new
    @f.tick
  end

  def validate
    @report = @f.validate if @report.nil?
  end

  def output_report
    validate
    rep = {}
    @report.each do |from, to, path, labels, reason, final|
      rep[to] ||= "Bot"
      rep[to] = disjunction(rep[to], final.last)
    end
    rep
  end

  def path_report
    validate
    zips = {}
    @report.each do |from, to, path, labels, reason, final|
      zips[to] ||= {}
      zips[to][from] ||= "Bot"
      zips[to][from] = disjunction(zips[to][from], final.last)
    end
    zips
  end
  
  def disjunction(l, r)
    both = [l, r]
    if both.include? "D"
      "D"
    elsif both.include? "N"
      if both.include? "A"
        return "D"
      else
        return "N"
      end
    elsif both.include? "A"
      return "A"
    else
      return "Bot"
    end
  end

  def new_class(mod)
    Class.new do
      include Bud
      include MetaMods
      include mod
    end
  end
  
  def new_class_from_class(cls)
    Class.new(cls) do
      include MetaMods
    end
  end

  def internal_tabs
    cls = Class.new do
      include Bud
      include MetaMods
    end
    cls.new.tables.keys
  end
  
  def write_graph(fmt=:pdf)
    f.finish(internal_tabs, "#{@mod.to_s}.#{fmt}", fmt)
  end
end
