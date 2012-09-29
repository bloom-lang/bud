#require 'bloomgraph'

module PDG
  # a bloomgraph program that plots a NM-and-async-aware PDG
  state do
    scratch :bodies, [:table] => [:type]
    scratch :source, [:pred]
    scratch :sink, [:pred]
  end

  bloom do
    bodies <= dep{|d| [d.body, coll_type(d.body)]}
    bodies <= dep{|d| [d.head, coll_type(d.head)]}

    bnode <= bodies do |b| 
      shape = case b.type
        when Bud::BudTable then "rectangle"
        when Bud::LatticeWrapper then "triangle"
        else "oval"
      end
      [b.table, {:shape => shape}]
    end

    bedge <= dep do |d|
      line = d.label == "A" ? "dashed" : "solid"
      circle = d.label == "N" ? "veeodot" : "normal"
      [d.body, d.head, {:style => line, :arrowhead => circle, :penwidth => 4}]
    end
  end

  bloom :endpoints do
    source <= t_provides do |p|
      if p.input and !dep_tc.map{|d| d.head}.include? p.interface
        [p.interface]
      end
    end

    sink <= t_provides do |p|
      if !p.input and !dep_tc.map{|d| d.body}.include? p.interface
        [p.interface]
      end
    end

    bedge <= source{|s| ["S", s.pred, {}]}
    bedge <= sink{|s| [s.pred, "T", {}]}
  end

  bootstrap do
    bnode << ["S", {:shape => "diamond", :color => "blue"}]
    bnode << ["T", {:shape => "diamond", :color => "blue"}]
  end

  def coll_type(cn)
    tab = self.tables[cn.to_sym]
    if tab.nil?
      tab = self.lattices[cn.to_sym]
      if tab.nil?
        return nil
      end
    end
    tab.class
  end
end

