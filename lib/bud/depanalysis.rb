require 'rubygems'
require 'bud'

class DepAnalysis #:nodoc: all
  include Bud

  state do
    # Data inserted by client, usually from t_depends and t_provides
    scratch :depends, [:lhs, :op, :body, :neg, :in_body]
    scratch :providing, [:pred, :input]

    # Intermediate state
    scratch :depends_clean, [:lhs, :body, :neg, :temporal]

    scratch :depends_tc, [:lhs, :body, :via, :neg, :temporal]
    scratch :cycle, [:pred, :via, :neg, :temporal]
    scratch :underspecified, [:pred, :input]
    scratch :source, [:pred]
    scratch :sink, [:pred]
  end

  bloom :analysis do
    depends_clean <= depends do |d|
      is_temporal = (d.op.to_s =~ /<[\+\-\~]/)
      [d.lhs, d.body, d.neg, is_temporal]
    end

    # Compute the transitive closure of "depends_clean" to detect cycles in
    # the deductive fragment of the program.
    depends_tc <= depends_clean do |d|
      [d.lhs, d.body, d.body, d.neg, d.temporal]
    end
    depends_tc <= (depends_clean * depends_tc).pairs(:body => :lhs) do |b, r|
      [b.lhs, r.body, b.body, (b.neg or r.neg), (b.temporal or r.temporal)]
    end

    cycle <= depends_tc do |d|
      if d.lhs == d.body
        unless d.neg and !d.temporal
          [d.lhs, d.via, d.neg, d.temporal]
        end
      end
    end

    source <= providing do |p|
      if p.input and !depends_tc.map{|d| d.lhs}.include? p.pred
        [p.pred]
      end
    end

    sink <= providing do |p|
      if !p.input and !depends_tc.map{|d| d.body}.include? p.pred
        [p.pred]
      end
    end

    underspecified <= providing do |p|
      if p.input
        unless depends_tc.map{|d| d.body if d.lhs != d.body}.include? p.pred
          [p.pred, true]
        end
      else
        unless depends_tc.map{|dt| dt.lhs if dt.lhs != dt.body}.include? p.pred
          [p.pred, false]
        end
      end
    end
  end
end
