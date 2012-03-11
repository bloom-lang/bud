require 'rubygems'
require 'bud'

module MetaAlgebra
  state do
    table :alg_path, [:from, :to, :path, :last_rule, :tag, :lastop]
    scratch :clean_dep, [:body, :head, :rule_id] => [:tag, :lastop]

    scratch :rule_nm, [:rule_id] => [:tag]

    table :apj1, [:from, :head, :rule_id, :path, :tag, :tag2, :lastop]
    table :apj2, [:from, :head, :rule_id] => [:path, :tag, :lastop]
    table :seq_lattice, [:left, :right, :directional]
    table :seq_lattice_closure, [:left, :right, :directional, :dist]
    table :lub, [:left, :right] => [:result]
    table :seq_lattice_result, [:left, :right, :result]
    table :upper_bound, [:left, :right, :bound, :height]
    table :lps, alg_path.schema
  end

  bootstrap do
    seq_lattice <= [
      [:M, :A, false],
      [:M, :N, false],
      [:A, :D, false],
      [:N, :D, false],
      [:N, :A, true] #,

      # disabled, for now
      #[:A, :D, true],
      #[:D, :G, false],
      #[:A, :G, false]
    ]

  end

  def max_of(a, b)
    if b > a
      b
    else
      a
    end
  end
  
  bloom :debug do
    #stdio <~ upper_bound{|b| ["UPPERB: #{b.inspect}"]}
    #stdio <~ seq_lattice_closure{|c| ["SLC: #{c.inspect}"]}
    #stdio <~ jlr {|j| ["JLR: #{j.inspect}"]}
    #stdio <~ lub {|l| ["LUB #{l.inspect}, left class #{l.left.class}"]}
    #stdio <~ clean_dep.inspected
  end

  bloom :lattice_rules do
    seq_lattice_closure <= seq_lattice {|l| [l.left, l.right, l.directional, 1]}
    seq_lattice_closure <= seq_lattice {|l| [l.left, l.left, false, 0]}
    seq_lattice_closure <= seq_lattice {|l| [l.right, l.right, false, 0]}
    seq_lattice_closure <= (seq_lattice_closure * seq_lattice).pairs(:right => :left) do |c, l|
      [c.left, l.right, (c.directional or l.directional), c.dist + 1]
    end

    # the join lattice is symmetric
    lub <= seq_lattice_closure {|l| [l.left, l.right, l.right]}
    lub <= seq_lattice_closure {|l| [l.right, l.left, l.right] unless l.directional}

    # still need a LUB for incomparable types.
    upper_bound <= (seq_lattice_closure * seq_lattice_closure).map do |c1, c2|
      if c1.right == c2.right and seq_lattice_closure.find_all{|c| c.left == c1.left and c.right == c2.left}.empty?
        unless c1.left == c1.right or c2.left == c2.right
          [c1.left, c2.left, c1.right, max_of(c1.dist, c2.dist) + 1]
        end
      end
    end

    temp :jlr  <= upper_bound.argagg(:min, [upper_bound.left, upper_bound.right], upper_bound.height)
    lub <+ jlr {|j| [j.left, j.right, j.bound] unless lub.map{|l| [l.left, l.right]}.include? [j.left, j.right] }
  end

  def get_tag(nm, op)
    if nm and op == '<~'
      :D
    elsif nm
      :N
    elsif op == '<~'
      :A
    else
      :M
    end
  end

  def in_prefix(node, path)
    path.split("|").include? node
  end
 
  bloom :make_paths do
    rule_nm <= t_depends.reduce(Hash.new) do |memo, i|
      tag = get_tag(i.nm, i.op)
      if memo[i.rule_id].nil? or memo[i.rule_id] == :M
        memo[i.rule_id] = tag
      end
      memo
    end

    clean_dep <= (t_depends * rule_nm).pairs(:rule_id => :rule_id) do |dep, rn|
      unless dep.lhs == 'alg_path'
        [dep.body, dep.lhs, dep.rule_id, rn.tag, dep.op]
      end
    end

    alg_path <= clean_dep.map do |dep|
      [dep.body, dep.head, "#{dep.body}|#{dep.head}", dep.rule_id, dep.tag, dep.lastop]
    end

    lps <= (alg_path * t_provides).pairs(:from => :interface) do |a, p|
      if p.input
        a
      end
    end

    apj1 <= (alg_path * clean_dep).pairs(:to => :body) do |a, c|
      [a.from, c.head, c.rule_id, a.path, a.tag, c.tag, a.lastop]
    end
    apj2 <= (apj1 * lub).pairs(:tag => :left, :tag2 => :right)
    alg_path <= apj2.map do |p, l|
      unless in_prefix(p.head, p.path)
        [p.from, p.head, "#{p.path}|#{p.head}", p.rule_id, l.result, p.lastop]
      end
    end
  end 
end

module MetaReports
  state do
    table :global_property, [:from, :to, :tag, :c1, :c2]
    scratch :paths, [:from, :to] => [:cnt]
    table :tags, [:from, :to, :tag, :cnt]
    table :d_begins, [:from, :tag, :path, :len, :lastop]
    table :ap, d_begins.schema
    scratch :a_preds, d_begins.schema + [:fullpath]
  end

  bloom :loci do
    # one approach: for every paths that 'turns D', identify the last async edge before
    # the critical transition.  ordering this edge prevents diffluence.
    # find the first point of diffluence in each paths: d_begins already does this.
    # for each "D"-entry in d_begins, find the longest subpath ending in an async rule.
    a_preds <= (d_begins * ap).pairs(:from => :from) do |b, a|
      if a.len < b.len and a.tag == :A and b.path.index(a.path) == 0 and a.lastop == "<~" and b.tag == :D
        [a.from, a.tag, a.path, a.len, a.lastop, b.path]
      end
    end
  end

  bloom do
    paths <= alg_path.group([:from, :to], count(:tag))
    tags <= alg_path.group([:from, :to, :tag], count())
    global_property <= (paths * tags).pairs(:from => :from, :to => :to, :cnt => :cnt) do |p, t|
      [t.from, t.to, t.tag, p.cnt, t.cnt]
    end

    ap <= (alg_path * t_provides).pairs(:from => :interface) do |p, pr| 
      if pr.input
        [p.from, p.tag, p.path, p.path.split("|").length, p.lastop]
      end
    end

    d_begins <= ap.argagg(:min, [:from, :tag], :len)
  end
end

